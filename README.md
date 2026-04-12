# IPTV Viewing History — End-to-End Analytics Pipeline
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-Lakehouse-FF9900?logo=amazons3&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS_Glue-PySpark-FF9900?logo=amazonaws&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)
![Redshift](https://img.shields.io/badge/Redshift-Serverless-8C4FFF?logo=amazonredshift&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform&logoColor=white)

## Mục lục

- [Tổng quan](#tổng-quan)
  - [Sơ đồ kiến trúc (Architecture Diagram)](#sơ-đồ-kiến-trúc-architecture-diagram)
  - [Vấn đề đặt ra](#vấn-đề-đặt-ra)
  - [Mục tiêu](#mục-tiêu)
  - [Giá trị mang lại](#giá-trị-mang-lại)
  - [Giai đoạn hiện tại](#giai-đoạn-hiện-tại)
  - [Trực quan dữ liệu (Data Visualization)](#trực-quan-dữ-liệu-data-visualization)

- [Kiến trúc hệ thống (Architecture)](#kiến-trúc-hệ-thống-architecture)
- [Công nghệ sử dụng (Tech Stack)](#công-nghệ-sử-dụng-tech-stack)
- [Cấu trúc project (Project Structure)](#cấu-trúc-project-project-structure)

- [Lược đồ dữ liệu (Data Schema)](#lược-đồ-dữ-liệu-data-schema)
  - [Dữ liệu thô — Bronze Layer](#dữ-liệu-thô--bronze-layer)
  - [Dữ liệu sạch — Silver Layer](#dữ-liệu-sạch--silver-layer-parquet)
  - [Dữ liệu phân tích — Gold Layer](#dữ-liệu-phân-tích--gold-layer-redshift)

- [Pipeline Airflow — DAG](#pipeline-airflow--dag-iptv_pipeline)

- [Cách chạy (How to run)](#cách-chạy-how-to-run)
  - [Yêu cầu](#yêu-cầu)
  - [Các bước](#các-bước)

- [Kết quả](#kết-quả)
- [Bài học kinh nghiệm (Lessons Learned)](#bài-học-kinh-nghiệm-lessons-learned)
- [Triển khai & Mở rộng (Production & Future Improvements)](#triển-khai--mở-rộng-production--future-improvements)
- [Liên hệ (Contact)](#liên-hệ-contact)


---
## Tổng quan
### Sơ đồ kiến trúc (Architecture Diagram)
![Cloud-Native Data Lakehouse Architecture](images/architecture.png)
### Vấn đề đặt ra

Các nhà cung cấp IPTV thu thập hàng triệu bản ghi log xem truyền hình thô mỗi ngày, nhưng dữ liệu rất hỗn loạn: thiếu các trường thông tin, thời lượng không hợp lệ, các sự kiện bị trùng lặp và nghi ngờ có các hợp đồng gian lận chia sẻ quá nhiều thiết bị. Nếu không có một pipeline đáng tin cậy, đội ngũ nội dung và kinh doanh không thể trả lời các câu hỏi cơ bản như:

- Ứng dụng/kênh nào giữ chân người xem lâu nhất mỗi ngày?
- Có bao nhiêu subscribers duy nhất đang hoạt động?
- Những hợp đồng nào cho thấy hành vi đa thiết bị bất thường (nghi vấn gian lận)?

### Mục tiêu

Xây dựng một Data Lakehouse pipeline hoàn toàn tự động trên AWS để nạp dữ liệu IPTV log thô hàng ngày, làm sạch và biến đổi dữ liệu thông qua Medallion architecture (Bronze → Silver → Gold), nạp vào Redshift và cung cấp các bảng sẵn sàng cho phân tích thông qua dbt, tất cả được điều phối bởi Apache Airflow.

### Giá trị mang lại
Pipeline này giúp:
- Theo dõi mức độ engagement của người dùng theo từng kênh
- Phát hiện hành vi chia sẻ tài khoản bất thường
- Hỗ trợ đội business tối ưu nội dung và chiến lược giữ chân người dùng

### Giai đoạn hiện tại
> **Repository này mô tả giai đoạn chạy pipeline trong môi trường Docker cục bộ.**
> Toàn bộ hạ tầng AWS (EC2, Glue, Redshift Serverless) được định nghĩa qua Terraform và đã sẵn sàng để triển khai. Logic của pipeline đã hoạt động đầy đủ và được kiểm thử end-to-end trong Docker.

### Trực quan dữ liệu (Data Visualization)
![User Engagement Dashboard](images/powerbi-dashboard.png)
---

## Kiến trúc hệ thống (Architecture)
Pipeline xử lý theo batch hàng ngày:
S3 (Landing) → Glue (ETL) → S3 (Silver) → Redshift → dbt (Gold)
```
Raw JSON Logs 
        │
        ▼
  ┌─────────────┐
  │  Landing/   │  ← Daily JSON file dropped here (S3)
  └──────┬──────┘
         │  Airflow: check_and_prepare
         ▼
  ┌─────────────┐
  │   Bronze/   │  ← Dữ liệu thô sẽ chuyển vào đây và được phân vùng theo date
  └──────┬──────┘
         │  AWS Glue (PySpark)
         ▼
  ┌─────────────┐
  │   Silver/   │  ← Làm sạch, chuẩn hóa, đánh nhãn gian lận
  └──────┬──────┘
         │  Redshift COPY
         ▼
  ┌──────────────────────┐
  │  staging.viewing_    │  ← Bảng staging thô trong Redshift
  │       history        │
  └──────┬───────────────┘
         │  dbt run
         ▼
  ┌──────────────────────┐
  │  mart.fct_daily_     │  ← Bảng Gold sẵn sàng phân tích
  │       views          │
  └──────────────────────┘
```

**Điều phối pipeline:** Apache Airflow lập lịch và giám sát mọi bước trên. \
**Quản lý hạ tầng:** Terraform quản lý tất cả tài nguyên AWS (S3, IAM roles, Glue job, Redshift Serverless).

---

## Công nghệ sử dụng (Tech Stack)

| Lớp           | Công nghệ                          | Mục đích                                  |
|------------------|--------------------------------------|------------------------------------------|
| Orchestration    | Apache Airflow 2.x (Docker)          | Lập lịch DAG, backfill, giám sát    |
| Storage          | AWS S3                               | Medallion Lakehouse (Landing/Bronze/Silver) |
| Processing       | AWS Glue + PySpark                   | Làm sạch dữ liệu, phát hiện gian lận           |
| Transformation   | dbt-core 1.7 + dbt-redshift          | Mô hình hóa SQL, kiểm thử, lineage           |
| Warehouse        | AWS Redshift Serverless              | Lớp truy vấn phân tích                 |
| Containerization | Docker + Docker Compose              | Phát triển và kiểm thử cục bộ            |
| IaC              | Terraform                            | Hạ tầng AWS dưới dạng mã nguồn             |
| Language         | Python 3.12                          | Airflow DAGs, mã nạp dữ liệu          |

**Lý do lựa chọn stack (Design Decisions)**
- AWS Glue & Redshift Serverless: Mô hình NoOps, tự động mở rộng và chỉ trả tiền khi sử dụng.
    - Note: Có thể thay Redshift bằng Athena để tối ưu chi phí (Pay-per-query), chấp nhận đánh đổi về độ trễ (latency).
- dbt: Áp dụng tư duy kỹ thuật phần mềm vào SQL (Lineage, Testing, Version Control).
- Apache Airflow: Điều phối các phụ thuộc (Dependencies) phức tạp, hỗ trợ Backfill và Retry mạnh mẽ.
- Terraform: Đảm bảo tính nhất quán của hạ tầng, dễ dàng tái bản môi trường qua code.
---

## Cấu trúc project (Project Structure)

```
IPTV_DE/
├── airflow/
│   ├── dags/
│   │   └── iptv_daily_pipeline.py   # Main Airflow DAG
│   ├── logs/                         # Airflow task logs
│   └── plugins/                      # Custom Airflow plugins 
│
├── ingestion/
│   └── upload_to_s3.py               # Script to upload raw JSON to S3 landing/
│
├── iptv_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_viewing_history.sql   # Staging view (clean & rename)
│   │   └── mart/
│   │       └── fct_daily_views.sql       # Gold fact table (daily aggregation)
│   ├── tests/                            # dbt data quality tests
│   ├── dbt_project.yml
│   └── profiles.yml                      # Redshift connection config
│
├── terraform/
│   ├── main.tf                       # S3, IAM, Glue job, Redshift
│   ├── variables.tf
│   └── outputs.tf
│
├── src/
│   └── dataprofiling.py              # Exploratory data profiling script
│
├── docker-compose.yml                # Airflow local environment
├── requirements.txt                  # Python dependencies
└── .env.example                      # Environment variable template
```

---

## Lược đồ dữ liệu (Data Schema)

### Dữ liệu thô — Bronze Layer

Các file JSON được ingest hàng ngày từ hệ thống IPTV.
Mỗi record tương ứng với một sự kiện xem của người dùng.

| Field           | Type   | Mô tả                                             |
| --------------- | ------ | ------------------------------------------------- |
| `_index`        | string | Tên index trong Elasticsearch                     |
| `_type`         | string | Loại document                                     |
| `_id`           | string | ID sự kiện gốc (sẽ được đổi tên thành `event_id`) |
| `Contract`      | string | ID thuê bao (khách hàng)                          |
| `Mac`           | string | Địa chỉ MAC thiết bị (có thể chứa dấu phẩy)       |
| `TotalDuration` | long   | Thời gian xem (giây, dữ liệu thô chưa validate)   |
| `AppName`       | string | Tên ứng dụng/kênh được xem                        |
| `date`          | date   | Ngày phát sinh log                                |


### Dữ liệu sạch — Silver Layer (Parquet)

Output từ job AWS Glue, được lưu trên S3 và partition theo year/month/day.

| Field                    | Type    | Mô tả                                                      |
| ------------------------ | ------- | ---------------------------------------------------------- |
| `event_id`               | string  | ID sự kiện sau khi làm sạch (loại bỏ null)                 |
| `contract_id`            | string  | ID khách hàng (đã loại bỏ null)                            |
| `device_mac`             | string  | MAC đã chuẩn hóa (viết hoa, bỏ dấu phẩy)                   |
| `app_name`               | string  | Tên kênh/app (đã loại bỏ app test BHD)                     |
| `total_duration_seconds` | integer | Thời gian xem hợp lệ (0 < x ≤ 86400)                       |
| `batch_date`             | string  | Ngày xử lý dữ liệu (YYYYMMDD)                              |
| `fraud_label`            | string  | Nhãn gian lận: `normal` / `multi_device` / `fraud_suspect` |


**Các bước xử lý trong Glue job:**
- Đổi tên và ép kiểu các trường theo schema chuẩn
- Loại bỏ các bản ghi null/empty ở event_id, contract_id, device_mac
- Lọc dữ liệu với điều kiện: 0 < total_duration_seconds ≤ 86400
- Loại bỏ dữ liệu test (AppName = 'BHD')
- Chuẩn hóa MAC address (viết hoa, bỏ dấu phẩy)
- Loại bỏ bản ghi trùng dựa trên event_id
- Phát hiện gian lận: đánh dấu các contract có số lượng MAC ≥ fraud_mac_threshold

### Dữ liệu phân tích — Gold Layer (Redshift)
**Grain:** 1 record / (batch_date, app_name)

**`staging.stg_viewing_history`** — Staging view (dbt)

| Field                    | Type                   | Mô tả |
|--------------------------|------------------------|------|
| `event_id`               | varchar(255)           | ID sự kiện duy nhất |
| `contract_id`            | varchar(255)           | ID thuê bao |
| `total_duration_seconds` | integer                | Thời gian xem (giây) |
| `app_name`               | varchar(255)           | Tên kênh/app |
| `batch_date`             | varchar(20)            | Ngày xử lý (YYYYMMDD) |
| `event_date`             | date                   | Ngày phát sinh sự kiện |
| `device_mac`             | varchar(50)            | MAC thiết bị đã chuẩn hóa |
| `device_flag`            | varchar(50)            | Nhãn phân loại thiết bị (phục vụ phát hiện gian lận) |


**`mart.fct_daily_views`** — Daily fact table (dbt)

| Field                     | Mô tả                                        |
| ------------------------- | -------------------------------------------- |
| `pk`                      | Khóa surrogate từ (`batch_date`, `app_name`) |
| `batch_date`              | Ngày xử lý (YYYYMMDD)                        |
| `app_name`                | Tên kênh/app                                 |
| `total_sessions`          | Tổng số phiên xem                            |
| `unique_contracts`        | Số lượng thuê bao duy nhất                   |
| `unique_devices`          | Số lượng thiết bị duy nhất                   |
| `total_seconds`           | Tổng thời gian xem (giây)                    |
| `total_hours`             | Tổng thời gian xem (giờ)                     |
| `avg_minutes_per_session` | Thời gian xem trung bình mỗi phiên (phút)    |

---

## Pipeline Airflow — DAG: iptv_pipeline
- Các task được cấu hình retry và timeout để đảm bảo pipeline ổn định

![Airflow DAG Graph - Success](images/airflow-dag-success.png)
![Pipeline Audit Log](images/audit-log.png)

| Task                 | Mô tả                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `check_and_prepare`  | Đọc `ds_nodash` từ context của Airflow. Kiểm tra dữ liệu ở Landing → chuyển sang Bronze, hoặc xác nhận Bronze đã tồn tại để rerun/backfill |
| `trigger_glue_job`   | Khởi chạy job AWS Glue (PySpark) với input/output trên S3 và tham số fraud threshold                                                       |
| `wait_glue_complete` | Kiểm tra trạng thái job Glue mỗi 30s cho đến khi hoàn thành hoặc thất bại (tối đa 1 giờ)                                                   |
| `copy_to_redshift`   | Xóa dữ liệu cũ theo `batch_date` trong staging, sau đó COPY dữ liệu từ Silver (Parquet)                                                    |
| `prepare_dbt_vars`   | Đẩy biến `batch_date_sql` vào XCom để dùng cho dbt                                                                                         |
| `run_dbt`            | Chạy các model dbt (`stg_viewing_history`, `fct_daily_views`)                                                                              |
| `test_dbt`           | Chạy test dbt để kiểm tra chất lượng dữ liệu                                                                                               |


---
## Cách chạy (How to run)

### Yêu cầu

- Docker Desktop installed and running
- AWS account with S3, Glue, Redshift Serverless configured (or Terraform deployed)
- Python 3.12+

### Các bước

**1. Clone the repository**
```bash
git clone https://github.com/your-username/IPTV_DE.git
cd IPTV_DE
```

**2. Tạo file .env**
```bash
cp .env.example .env
```

Thêm vào file `.env` với các biến của bạn
```env
S3_BUCKET=your-bucket-name
REDSHIFT_WORKGROUP=your-workgroup
REDSHIFT_DATABASE=your-database
REDSHIFT_IAM_ROLE=arn:aws:iam::123456789:role/your-role
AWS_DEFAULT_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
DBT_PROJECT_DIR=/opt/airflow/iptv_dbt
DBT_PROFILES_DIR=/opt/airflow/iptv_dbt
```

**3. Khởi chạy airflow**
```bash
docker-compose up -d
```
Đợi khoảng 30-60 giây rồi kiểm tra các container đã healthy chưa:
```bash
docker ps
```
Đảm bảo thấy `airflow-scheduler`, `airflow-webserver`, `airflow-worker` đều ở trạng thái `healthy`.

**4. Truy cập Airflow UI**

Mở `http://localhost:8080` — Nhập: `airflow / airflow`

**5. Upload raw data lên S3 landing**
```bash
python ingestion/upload_to_s3.py --date 2022-04-01
```

**6. Khởi chạy pipeline (3 lựa chọn)**

- Option A: Tự động (Scheduler): Dành cho dữ liệu hằng ngày.
Airflow sẽ tự động chạy mỗi ngày, đọc batch_date từ execution_date
và tìm file tương ứng trong bronze/ hoặc landing/ trên S3.

- Option B: Chạy bù dữ liệu lịch sử (Backfill): Dành cho nạp dữ liệu quá khứ.
```bash
# Cần Pause DAG trên UI trước khi thực hiện lệnh CLI:
# Trường hợp 1 — Chạy lần đầu
docker exec -it airflow-airflow-scheduler-1 \
  airflow dags backfill -s 2022-04-01 -e 2022-04-04 iptv_pipeline

# Trường hợp 2 — Reset và chạy lại
docker exec -it airflow-airflow-scheduler-1 \
  airflow dags backfill -s 2022-04-01 -e 2022-04-04 --reset-dagruns iptv_pipeline
```

- Option C: Kích hoạt thủ công (Manual Trigger): Dành cho kiểm thử (Testing). Trên UI, chọn Trigger DAG w/ config và nhập ngày cụ thể: "batch_date": "2022-04-01"


---

## Kết quả

Truy vấn từ `mart.fct_daily_views` sau khi hoàn thành pipeline:

![Analytics Mart - Fact Daily Views](images/analytics-mart.png)

---

## Bài học kinh nghiệm (Lessons Learned)

**1. Airflow XCom & thiết kế luồng xử lý đơn (single-path)**
- Ban đầu DAG sử dụng BranchPythonOperator với hai nhánh manual và auto, dẫn đến lỗi XCom “silent” khi thiếu task_ids trong xcom_pull. Sau đó refactor về một task duy nhất check_and_prepare sử dụng ds_nodash — giúp đơn giản hóa logic, giảm lỗi và dễ debug hơn. 

**2.Catchup và Backfill qua CLI**
- Việc đặt catchup=True mà không có end_date có thể tạo ra hàng nghìn DAG run không mong muốn từ start_date. Best practice là để catchup=False cho scheduler, và sử dụng CLI airflow dags backfill với khoảng thời gian cụ thể khi cần chạy lại dữ liệu lịch sử. Luôn pause DAG trước khi backfill để tránh xung đột.

**3. dbt không nằm trong PATH của BashOperator**
- BashOperator chạy trong môi trường shell tối giản, không load virtualenv nên không tìm thấy dbt dù đã cài đặt. Cách xử lý là sử dụng đường dẫn tuyệt đối đến binary của dbt (ví dụ: /home/airflow/.local/bin/dbt) hoặc truyền qua biến môi trường.

**4. Vấn đề concurrency và lock trong Redshift**
- Chạy nhiều DAG run song song cùng thực hiện lệnh COPY vào một bảng staging gây ra lock contention. Giải pháp là đặt max_active_runs=1 để đảm bảo các batch chạy tuần tự, tránh timeout trong Redshift.

**5. Trạng thái container Docker không được lưu trữ**
- Các package cài trực tiếp trong container sẽ bị mất sau khi restart. Vì vậy, cần khai báo toàn bộ dependencies trong requirements.txt để đảm bảo mỗi lần build lại đều reproducible.

---

## Triển khai & Mở rộng (Production & Future Improvements)

- [ ] Triển khai toàn bộ hạ tầng lên AWS (EC2, S3, Glue, Redshift) thông qua terraform apply
- [ ] Tích hợp Great Expectations để kiểm tra chất lượng dữ liệu đầu vào tại Bronze layer
- [ ] Xây dựng dashboard giám sát trên AWS QuickSight hoặc Metabase
- [ ] Thiết lập hệ thống cảnh báo (alerting) qua SNS khi pipeline gặp lỗi
- [ ] Mở rộng logic phát hiện gian lận bằng mô hình Machine Learning (anomaly detection)

---

## Liên hệ (Contact)

**Nguyen Phuc Vinh**
- Email: phucvinh235371@gmail.com
- LinkedIn: [linkedin.com/in/phucvinh235371](https://www.linkedin.com/in/phucvinh235371)


```
+===================================================================================+
|                    SPARK EXECUTION PIPELINE (BRONZE → SILVER)                      |
+===================================================================================+

~~~~~~~~~~~~~~~~~~~~~~~ VÙNG XANH: TRANSFORMATIONS (LẬP KẾ HOẠCH - LAZY) ~~~~~~~~~~~~
( Dữ liệu CHƯA chạy qua đây. Spark chỉ đang vẽ bản đồ thực thi - DAG )

   +---------------------+
   | 1. spark.read.json  |  <--- [ INPUT ] Quét schema từ S3 Bronze
   |   (S3 Bronze)       |
   +----------+----------+
              |
              v
   +---------------------+
   | 2. Select & Alias   |  <--- Định nghĩa lại cột (Mapping), thêm cột batch_date (lit)
   |   + F.lit(...)      |      (Cột hằng số được tính trước - Constant Folding)
   +----------+----------+
              |
              v
   +---------------------+
   | 3. WithColumn       |  <--- Thêm cột event_date từ file_date (lit)
   |   (event_date)      |
   +----------+----------+
              |
              |  [ Spark Optimizer: Gộp tất cả Filter thành 1 bước quét duy nhất ]
              v
   +---------------------+
   | 4. Data Cleaning    |  <--- [ PHỄU LỌC ] Pushdown xuống gần nguồn nhất có thể
   |  - NotNull filters  |      Loại bỏ NULL, rỗng, app test, duration sai
   |  - Duration (0..24h)|      (Các điều kiện được tối ưu chạy cùng lúc)
   |  - App_name != 'BHD'|
   +----------+----------+
              |
              v
   +---------------------+
   | 5. Normalization    |  <--- Chuẩn hóa MAC: bỏ dấu :, uppercase
   |   (regexp_replace   |      Kiểm tra MAC pattern hợp lệ (regex)
   |    + upper)         |
   +----------+----------+
              |
              v
   +---------------------+
   | 6. GroupBy & Join   |  <--- [ SHUFFLE ] Dữ liệu bay qua mạng giữa các Executor
   |  (mac_per_contract) |      - Tính COUNT(DISTINCT device_mac) theo contract_id
   |                     |      - Join ngược lại để gán flag (normal/multi/fraud)
   |   +-----------+     |
   |   |  SHUFFLE  | <---|------> Mạng lưới dữ liệu xáo trộn (Tốn tài nguyên nhất)
   |   +-----------+     |
   +----------+----------+
              |
              v
   +---------------------+
   | 7. .cache()         |  <--- [ ĐÁNH DẤU ] Vẫn là Transformation Lazy!
   |                     |      "Nếu dữ liệu được tính, hãy giữ nó trong RAM"
   +----------+----------+
              |
==============|==================== ĐƯỜNG ĐỨT QUÃNG ================================
              |                 (Bắt đầu thực sự chạy dữ liệu từ S3)
              v
   VÙNG ĐỎ: ACTIONS (KÍCH HOẠT THỰC THI - EAGER)
   ( Khi chạm nút này, Spark mới đọc dữ liệu và đẩy qua toàn bộ pipeline )

   +---------------------+
   | 8. .count()         |  <--- [ TRIGGER LẦN 1 ] 
   |                     |      Spark chạy toàn bộ bước 1→7. Kết quả đếm trả về Driver.
   |                     |      ĐỒNG THỜI: Dữ liệu sạch được lưu vào RAM (vì đã .cache() ở bước 7)
   +----------+----------+
              |
              +-----------------------------------------------+
              |                                               |
              v                                               v
   +---------------------+                         +-------------------------+
   | 9. Quality Reports  |                         | 10. df.write.parquet() |
   |  (các .count()      |                         |     (S3 Silver)         |
   |   riêng lẻ)         |                         |                         |
   |                     |                         |   [ TRIGGER LẦN 2 ]     |
   |  Vì đã có cache,    |                         |   Đọc dữ liệu từ cache  |
   |  các lần đếm này    |                         |   để ghi ra Parquet.    |
   |  LẤY TRỰC TIẾP TỪ   |                         |   Nếu không có cache,    |
   |  RAM, không chạy    |                         |   Spark sẽ đọc lại S3 &  |
   |  lại pipeline!      |                         |   chạy lại 1→7 lần nữa.  |
   +---------------------+                         +-------------------------+

   +---------------------+
   | 11. job.commit()    |  <--- Báo Glue job thành công, giải phóng tài nguyên
   +---------------------+


+===================================================================================+
|                               CHÚ THÍCH KỸ THUẬT                                  |
+===================================================================================+
| [Catalyst Optimizer]  | Spark tự động sắp xếp lại thứ tự các phép biến đổi để tối  |
|                       | ưu. Filter được đẩy lên sớm, Projection (select) được cắt  |
|                       | bớt cột không dùng. Thứ tự code ≠ thứ tự thực thi thực tế. |
+-----------------------+-----------------------------------------------------------+
| [Constant Folding]    | Với F.lit(batch_date) và to_date(lit(file_date)), Spark   |
|                       | tính giá trị một lần duy nhất, không phụ thuộc vào dữ liệu.|
+-----------------------+-----------------------------------------------------------+
| [Shuffle]             | Dữ liệu phải di chuyển qua mạng giữa các máy Executor khi   |
|                       | có groupBy, join, distinct. Đây là nguyên nhân chính gây     |
|                       | chậm job. Hạn chế shuffle là mục tiêu tối ưu hàng đầu.       |
+-----------------------+-----------------------------------------------------------+
| [Cache]               | .cache() không phải Action. Nó chỉ đánh dấu lưu trữ. Action  |
|                       | đầu tiên (count) mới tính dữ liệu và lưu vào RAM. Nếu không  |
|                       | cache, mỗi Action sau sẽ chạy lại toàn bộ pipeline từ S3.    |
+-----------------------+-----------------------------------------------------------+
```
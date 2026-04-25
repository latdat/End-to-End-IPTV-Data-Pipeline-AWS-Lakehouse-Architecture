import pandas as pd

ACTIVE_PREFIXES = {
    "B84DEE", "10394E", "08674E", "E4AB89", "B046FC",
    "84AA9C", "0C96E6", "CCEDDC", "485F99", "90324B",
    "4CEBBD", "D89C67", "8CC84B", "18473D", "802BF9",
    "B4B5B6", "7440BB", "D81265", "9C305B", "541379",
    "5CEA1D", "CCD4A1", "1CBFC0", "E86F38", "D46A6A",
    "B05216", "28565A", "405BD8", "402343", "DCA266",
    "105BAD", "945330", "A86BAD", "D80F99", "F8DA0C",
    "283A4D", "5C3A45", "5CBAEF", "B068E6", "C0B5D7",
    "2C6FC9", "ACD564", "D43A2F", "E89EB4", "FC017C",
    "90CDB6", "001C55", "9897D1", "D43A2E", "B8FFB3",
    "40490F", "AC4AFE", "0016E8", "404A03", "681401",
    "7CBA3F", "0015AA", "001FA9", "30F772", "00153C",
    "001538", "0015E8", "001535", "001582", "0015DE",
    "A4FC77", "001579", "0015AB", "00155D", "001543",
    "0015D0", "0015FE", "0015B9", "0015C2", "0015C4",
    "00152B", "00153A", "0015AE", "0015FD", "001568",
    "00159C", "00158C", "001589", "0015BA", "00153B",
    "00159E", "0015D6", "00157F", "001574", "001577",
    "001520", "0015F1", "00157E", "001519", "00154F",
    "00157B", "0015A3", "00156E", "001580", "001504"
}

# Đọc file gốc IEEE (~35k rows)
df_oui = pd.read_csv(
    r"D:\ProjectDE\IPTV_DE\iptv_dbt\seeds\oui.csv",
    usecols=["Assignment", "Organization Name", "Organization Address"],
    dtype=str
)

df_oui = df_oui.rename(columns={
    "Assignment":           "oui_hex",
    "Organization Name":    "manufacturer",
    "Organization Address": "address_raw"
})

# Xử lý khoảng trắng thừa/ký tự xuống dòng trong tên công ty và địa chỉ
df_oui["manufacturer"] = df_oui["manufacturer"].str.replace(r'\s+', ' ', regex=True).str.strip()
df_oui["address_raw"] = df_oui["address_raw"].str.replace(r'\s+', ' ', regex=True).str.strip()

# Tìm 2 chữ cái in hoa [A-Z]{2} mà phía sau nó có thể là khoảng trắng và một cụm số/chữ của Postal Code hoặc nó nằm ở cuối dòng.
df_oui["country_code"] = (
    df_oui["address_raw"]
    .str.extract(r'\b([A-Z]{2})(?:\s+[\w-]+)?\s*$')
)

# Lọc chỉ prefix có trong data thực
df_filtered = (
    df_oui[df_oui["oui_hex"].isin(ACTIVE_PREFIXES)]
    .sort_values("oui_hex")
    .reset_index(drop=True)
)

cols = ["oui_hex", "manufacturer", "country_code", "address_raw"]
df_filtered = df_filtered[cols]

print(f"Matched  : {len(df_filtered)} / {len(ACTIVE_PREFIXES)} prefixes")
print(f"Unmatched: {ACTIVE_PREFIXES - set(df_filtered['oui_hex'])}")

df_filtered.to_csv(r"D:\ProjectDE\IPTV_DE\iptv_dbt\seeds\oui_lookup.csv", index=False)
print("Saved => D:\ProjectDE\IPTV_DE\iptv_dbt\seeds\oui_lookup.csv")
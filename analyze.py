# -*- coding: utf-8 -*-
"""
analyze.py
Mengambil data dari Google Trends (4 jam terakhir) -> Simpan ke Big_Data
Membaca search_logs dari MongoDB -> Hitung top produk -> Simpan ke analytics_products
"""

import os
import time
import random
import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient, DESCENDING
from dotenv import load_dotenv
from pytrends.request import TrendReq

load_dotenv()

# KONEKSI
MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME   = os.environ.get("DB_APP", "anyaman")

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

col_search_logs     = db["search_logs"]
col_popular_keyword = db["PopularKeyword"]
col_analytics       = db["analytics_products"]
col_big_data        = db["Big_Data"]

# KONFIGURASI PYTRENDS
KEYWORDS = [
    "bilik bambu", "piring bambu", "keranjang bambu", "tas anyaman bambu",
    "caping bambu", "kipas sate", "tudung saji bambu", "bakul nasi bambu",
    "tampah bambu", "ceting bambu"
]

KATEGORI_MAP = {
    "bilik bambu": "Dekorasi", "piring bambu": "Perabot Makan",
    "keranjang bambu": "Wadah & Penyimpanan", "tas anyaman bambu": "Aksesori Fashion",
    "caping bambu": "Aksesori Kepala", "kipas sate": "Aksesori Pribadi",
    "tudung saji bambu": "Wadah & Penyimpanan", "bakul nasi bambu": "Perabot Makan",
    "tampah bambu": "Perabot Dapur", "ceting bambu": "Perabot Dapur"
}

REGION = "ID"
SOURCE = "Google Trends"
TIMEFRAME = "now 4-H"

print("Memulai pengambilan data dari Google Trends...")

pytrends = TrendReq(hl='id-ID', tz=420)
hasil_trends = []

for i, keyword in enumerate(KEYWORDS, 1):
    print(f"[{i}/{len(KEYWORDS)}] Mengambil: {keyword} ...")
    try:
        pytrends.build_payload(kw_list=[keyword], timeframe=TIMEFRAME, geo=REGION)
        data = pytrends.interest_over_time()

        if not data.empty:
            data = data.reset_index()
            if 'isPartial' in data.columns:
                data = data.drop(columns=['isPartial'])

            for _, row in data.iterrows():
                hasil_trends.append({
                    "Tanggal": row["date"],
                    "Keyword": keyword,
                    "Kategori": KATEGORI_MAP.get(keyword, "Lainnya"),
                    "Minat_Pencarian": int(row[keyword]),
                    "Region": REGION,
                    "Sumber": SOURCE,
                    "Waktu_Ambil": datetime.now(timezone.utc)
                })
    except Exception as e:
        print(f"Error pada keyword {keyword}: {e}")

    time.sleep(random.randint(10, 20))

# SIMPAN DATA TRENDS KE MONGODB
if hasil_trends:
    col_big_data.insert_many(hasil_trends)
    print(f"Berhasil menyimpan {len(hasil_trends)} baris data Trends ke Big_Data")
else:
    print("Tidak ada data Trends baru yang disimpan.")

# HITUNG TOP PRODUK DARI Big_Data
print("\nMenghitung top produk dari Big_Data ...")
pipeline_top = [
    {
        "$group": {
            "_id": "$Keyword",
            "kategori": {"$first": "$Kategori"},
            "jumlahDicari": {"$sum": "$Minat_Pencarian"},
        }
    },
    {"$sort": {"jumlahDicari": DESCENDING}},
    {"$limit": 10},
]

top_produk = list(col_big_data.aggregate(pipeline_top))
total = sum(p["jumlahDicari"] for p in top_produk)
print(f"{len(top_produk)} produk unik ditemukan")

# HITUNG STATISTIK DARI Big_Data
print("Menghitung statistik Google Trends ...")
trends_pipeline = [
    {
        "$group": {
            "_id": "$Keyword",
            "kategori": {"$first": "$Kategori"},
            "rataMinat": {"$avg": "$Minat_Pencarian"},
            "maxMinat": {"$max": "$Minat_Pencarian"},
            "totalData": {"$sum": 1}
        }
    },
    {"$sort": {"rataMinat": DESCENDING}}
]

trends_stats = list(col_big_data.aggregate(trends_pipeline))
print(f"{len(trends_stats)} keyword Google Trends diproses")

# SIMPAN KE analytics_products
print("Menyimpan hasil ke analytics_products ...")
now = datetime.now(timezone.utc)
col_analytics.delete_many({})

docs = []
for i, produk in enumerate(top_produk):
    trend_match = next(
        (t for t in trends_stats if produk["_id"].lower() in t["_id"].lower()), None
    )

    docs.append({
        "ranking": i + 1,
        "namaProduk": produk["_id"],
        "kategori": produk.get("kategori", "-"),
        "jumlahDicari": produk["jumlahDicari"],
        "persentase": round((produk["jumlahDicari"] / total * 100), 1) if total > 0 else 0,
        "rataMinatTrends": round(trend_match["rataMinat"], 1) if trend_match else None,
        "maxMinatTrends": trend_match["maxMinat"] if trend_match else None,
        "generatedAt": now,
        "sumber": "Google Trends Terpusat"
    })

if docs:
    col_analytics.insert_many(docs)
    print(f"{len(docs)} produk disimpan ke analytics_products")

# SYNC PopularKeyword dari Big_Data
print("\nSync PopularKeyword ...")
for produk in top_produk:
    col_popular_keyword.update_one(
        {"keyword": produk["_id"].lower()},
        {"$set": {
            "keyword": produk["_id"].lower(),
            "jumlahCari": produk["jumlahDicari"],
            "updatedAt": now,
        }},
        upsert=True,
    )
print(f"{len(top_produk)} keyword diperbarui")

# LAPORAN AKHIR
print("\n" + "=" * 50)
print(f"Analisis selesai: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("Top 3 produk:")
for doc in docs[:3]:
    print(f" #{doc['ranking']} {doc['namaProduk']} - {doc['jumlahDicari']}x dicari ({doc['persentase']}%)")
print("=" * 50)

client.close()
# -*- coding: utf-8 -*-
"""
analyze.py

Alur:
Google Trends
-> Python Script Data Collection
-> Cleaning + Anti Duplikasi
-> MongoDB Big_Data
-> Python Script Data Analytics
-> MongoDB analytics_products
-> Dashboard Aplikasi
-> GitHub Actions Schedule
"""

import os
import time
import random
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING
from pytrends.request import TrendReq

load_dotenv()

# =========================
# KONEKSI MONGODB
# =========================
MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ.get("DB_APP", "anyaman")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

col_big_data = db["Big_Data"]
col_analytics = db["analytics_products"]
col_popular_keyword = db["PopularKeyword"]

# =========================
# KONFIGURASI GOOGLE TRENDS
# =========================
KEYWORDS = [
    "bilik bambu",
    "piring bambu",
    "keranjang bambu",
    "tas anyaman bambu",
    "caping bambu",
    "kipas sate",
    "tudung saji bambu",
    "bakul nasi bambu",
    "tampah bambu",
    "ceting bambu",
]

KATEGORI_MAP = {
    "bilik bambu": "Dekorasi",
    "piring bambu": "Perabot Makan",
    "keranjang bambu": "Wadah & Penyimpanan",
    "tas anyaman bambu": "Aksesori Fashion",
    "caping bambu": "Aksesori Kepala",
    "kipas sate": "Aksesori Pribadi",
    "tudung saji bambu": "Wadah & Penyimpanan",
    "bakul nasi bambu": "Perabot Makan",
    "tampah bambu": "Perabot Dapur",
    "ceting bambu": "Perabot Dapur",
}

REGION = "ID"
SOURCE = "Google Trends"

# Karena GitHub Actions jalan setiap 3 jam,
# data trends diambil dari 4 jam terakhir agar tetap aman.
TIMEFRAME = "now 4-H"

now = datetime.now(timezone.utc)

print("=" * 60)
print("MEMULAI BIG DATA PIPELINE")
print(f"Waktu: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 60)

# =========================
# BUAT INDEX ANTI DUPLIKASI
# =========================
col_big_data.create_index(
    [("Tanggal", 1), ("Keyword", 1), ("Region", 1)],
    unique=True,
)

# =========================
# DATA COLLECTION
# =========================
print("\nMengambil data dari Google Trends...")

pytrends = TrendReq(hl="id-ID", tz=420)

total_insert = 0
total_duplicate = 0
total_error = 0

for i, keyword in enumerate(KEYWORDS, 1):
    print(f"[{i}/{len(KEYWORDS)}] Mengambil keyword: {keyword}")

    try:
        pytrends.build_payload(
            kw_list=[keyword],
            timeframe=TIMEFRAME,
            geo=REGION,
        )

        data = pytrends.interest_over_time()

        if data.empty:
            print(f"Data kosong untuk keyword: {keyword}")
            continue

        data = data.reset_index()

        if "isPartial" in data.columns:
            data = data.drop(columns=["isPartial"])

        for _, row in data.iterrows():
            tanggal = row["date"].to_pydatetime()

            if tanggal.tzinfo is None:
                tanggal = tanggal.replace(tzinfo=timezone.utc)

            minat = int(row[keyword])

            # CLEANING DATA
            cleaned_keyword = keyword.lower().strip()
            kategori = KATEGORI_MAP.get(cleaned_keyword, "Lainnya")

            document = {
                "Tanggal": tanggal,
                "Keyword": cleaned_keyword,
                "Kategori": kategori,
                "Minat_Pencarian": minat,
                "Region": REGION,
                "Sumber": SOURCE,
                "Waktu_Ambil": now,
            }

            # ANTI DUPLIKASI
            result = col_big_data.update_one(
                {
                    "Tanggal": tanggal,
                    "Keyword": cleaned_keyword,
                    "Region": REGION,
                },
                {
                    "$setOnInsert": document,
                },
                upsert=True,
            )

            if result.upserted_id:
                total_insert += 1
            else:
                total_duplicate += 1

        jeda = random.randint(10, 20)
        print(f"Berhasil diproses. Jeda {jeda} detik...\n")
        time.sleep(jeda)

    except Exception as e:
        total_error += 1
        print(f"Error pada keyword {keyword}: {e}")

print("\nDATA COLLECTION SELESAI")
print(f"Data baru       : {total_insert}")
print(f"Data duplikat   : {total_duplicate}")
print(f"Keyword error   : {total_error}")

# =========================
# DATA ANALYTICS
# =========================
print("\nMenghitung analisis produk...")

pipeline_top_produk = [
    {
        "$group": {
            "_id": "$Keyword",
            "kategori": {"$first": "$Kategori"},
            "jumlahDicari": {"$sum": "$Minat_Pencarian"},
            "rataMinat": {"$avg": "$Minat_Pencarian"},
            "maxMinat": {"$max": "$Minat_Pencarian"},
            "totalData": {"$sum": 1},
            "tanggalTerakhir": {"$max": "$Tanggal"},
        }
    },
    {
        "$sort": {
            "jumlahDicari": DESCENDING,
        }
    },
    {
        "$limit": 10,
    },
]

top_produk = list(col_big_data.aggregate(pipeline_top_produk))

total_pencarian = sum(item["jumlahDicari"] for item in top_produk)

analytics_docs = []

for index, item in enumerate(top_produk, start=1):
    jumlah_dicari = item["jumlahDicari"]

    persentase = (
        round((jumlah_dicari / total_pencarian) * 100, 1)
        if total_pencarian > 0
        else 0
    )

    analytics_docs.append(
        {
            "ranking": index,
            "namaProduk": item["_id"],
            "kategori": item.get("kategori", "-"),
            "jumlahDicari": jumlah_dicari,
            "persentase": persentase,
            "rataMinatTrends": round(item["rataMinat"], 1),
            "maxMinatTrends": item["maxMinat"],
            "totalData": item["totalData"],
            "tanggalTerakhir": item["tanggalTerakhir"],
            "generatedAt": now,
            "sumber": "Google Trends",
        }
    )

# =========================
# SIMPAN KE analytics_products
# =========================
print("Menyimpan hasil analisis ke analytics_products...")

col_analytics.delete_many({})

if analytics_docs:
    col_analytics.insert_many(analytics_docs)
    print(f"{len(analytics_docs)} data analytics berhasil disimpan.")
else:
    print("Tidak ada data analytics yang disimpan.")

# =========================
# SYNC KE PopularKeyword
# =========================
print("Sinkronisasi PopularKeyword...")

for item in analytics_docs:
    col_popular_keyword.update_one(
        {
            "keyword": item["namaProduk"],
        },
        {
            "$set": {
                "keyword": item["namaProduk"],
                "jumlahCari": item["jumlahDicari"],
                "ranking": item["ranking"],
                "kategori": item["kategori"],
                "updatedAt": now,
            }
        },
        upsert=True,
    )

# =========================
# LAPORAN AKHIR
# =========================
print("\n" + "=" * 60)
print("BIG DATA PIPELINE SELESAI")
print(f"Waktu selesai: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 60)

print("\nTop 3 Produk:")
for item in analytics_docs[:3]:
    print(
        f"#{item['ranking']} {item['namaProduk']} "
        f"- {item['jumlahDicari']} skor "
        f"({item['persentase']}%)"
    )

print("=" * 60)

client.close()
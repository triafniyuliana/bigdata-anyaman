# -*- coding: utf-8 -*-
"""
analyze.py

Alur Big Data:
1. Ambil produk dari MongoDB
2. Ambil keywordTrend dari produk
3. Data Collection dari Google Trends
4. Cleaning data
5. Anti duplikasi
6. Simpan data mentah ke Big_Data
7. Analisis data
8. Simpan hasil ke analytics_products
9. Sync PopularKeyword
10. Dashboard Admin / Flutter membaca hasil analisis
"""

import os
import time
import random
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING
from pytrends.request import TrendReq

load_dotenv()

# ======================================================
# 1. KONEKSI MONGODB
# ======================================================
MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ.get("DB_APP", "anyaman")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

col_produk = db["Produk"]
col_big_data = db["Big_Data"]
col_analytics = db["analytics_products"]
col_popular_keyword = db["PopularKeyword"]

REGION = "ID"
SOURCE = "Google Trends"
TIMEFRAME = "now 4-H"

now = datetime.now(timezone.utc)

print("=" * 60)
print("MEMULAI BIG DATA PIPELINE")
print(f"Waktu mulai: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 60)


# ======================================================
# 2. AMBIL PRODUK DARI DATABASE
# ======================================================
print("\nMengambil data produk dari MongoDB...")

produk_list = list(
    col_produk.find(
        {
            "keywordTrend": {
                "$exists": True,
                "$ne": "",
            }
        },
        {
            "_id": 1,
            "namaProduk": 1,
            "keywordTrend": 1,
            "kategori": 1,
        },
    )
)

if not produk_list:
    print("Tidak ada produk yang memiliki keywordTrend.")
    client.close()
    exit()

PRODUK_MAP = {}

for produk in produk_list:
    keyword = produk.get("keywordTrend", "").lower().strip()

    if keyword:
        PRODUK_MAP[keyword] = produk

KEYWORDS = list(PRODUK_MAP.keys())

print(f"Total produk dengan keywordTrend: {len(KEYWORDS)}")
print("Keyword yang akan digunakan untuk Google Trends:")

for keyword in KEYWORDS:
    produk = PRODUK_MAP[keyword]
    print(f"- {keyword} -> {produk.get('namaProduk')}")


# ======================================================
# 3. PERSIAPAN ANTI DUPLIKASI DATABASE
# ======================================================
def clean_duplicate_big_data():
    print("\nMembersihkan duplikasi lama di Big_Data...")

    pipeline = [
        {
            "$group": {
                "_id": {
                    "Tanggal": "$Tanggal",
                    "Keyword": "$Keyword",
                    "Region": "$Region",
                },
                "ids": {
                    "$push": "$_id",
                },
                "count": {
                    "$sum": 1,
                },
            }
        },
        {
            "$match": {
                "count": {
                    "$gt": 1,
                },
            }
        },
    ]

    duplicates = list(col_big_data.aggregate(pipeline))
    total_deleted = 0

    for item in duplicates:
        ids_to_delete = item["ids"][1:]

        if ids_to_delete:
            result = col_big_data.delete_many(
                {
                    "_id": {
                        "$in": ids_to_delete,
                    }
                }
            )

            total_deleted += result.deleted_count

    print(f"Duplikasi lama dihapus: {total_deleted}")


clean_duplicate_big_data()

col_big_data.create_index(
    [
        ("Tanggal", 1),
        ("Keyword", 1),
        ("Region", 1),
    ],
    unique=True,
)

print("Index anti duplikasi aktif: Tanggal + Keyword + Region")


# ======================================================
# 4. DATA COLLECTION DARI GOOGLE TRENDS
# ======================================================
print("\nMengambil data dari Google Trends...")

pytrends = TrendReq(
    hl="id-ID",
    tz=420,
)

hasil_collection = []
total_error = 0

for i, keyword in enumerate(KEYWORDS, 1):
    produk = PRODUK_MAP[keyword]

    print(
        f"[{i}/{len(KEYWORDS)}] Mengambil keyword: {keyword} "
        f"untuk produk: {produk.get('namaProduk')}"
    )

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

        hasil_collection.append(
            {
                "keyword": keyword,
                "produk": produk,
                "data": data,
            }
        )

        print(f"Data berhasil diambil: {len(data)} baris")

        jeda = random.randint(10, 20)
        print(f"Jeda {jeda} detik...\n")
        time.sleep(jeda)

    except Exception as e:
        total_error += 1
        print(f"Error pada keyword {keyword}: {e}")


# ======================================================
# 5. CLEANING DATA
# ======================================================
print("\nMelakukan cleaning data...")

cleaned_documents = []

for item in hasil_collection:
    keyword = item["keyword"]
    produk = item["produk"]
    data = item["data"]

    if "isPartial" in data.columns:
        data = data.drop(
            columns=["isPartial"],
        )

    for _, row in data.iterrows():
        tanggal = row["date"].to_pydatetime()

        if tanggal.tzinfo is None:
            tanggal = tanggal.replace(
                tzinfo=timezone.utc,
            )

        cleaned_keyword = keyword.lower().strip()
        minat = int(row[keyword])

        document = {
            "Tanggal": tanggal,
            "Keyword": cleaned_keyword,
            "Kategori": produk.get("kategori", "Lainnya"),
            "Minat_Pencarian": minat,
            "Region": REGION,
            "Sumber": SOURCE,
            "Waktu_Ambil": now,

            # Penghubung ke produk asli aplikasi
            "produkId": str(produk["_id"]),
            "namaProduk": produk.get("namaProduk"),
        }

        cleaned_documents.append(document)

print(f"Total data setelah cleaning: {len(cleaned_documents)}")


# ======================================================
# 6. SIMPAN KE Big_Data DENGAN ANTI DUPLIKASI
# ======================================================
print("\nMenyimpan data mentah ke Big_Data...")

total_insert = 0
total_duplicate = 0

for document in cleaned_documents:
    result = col_big_data.update_one(
        {
            "Tanggal": document["Tanggal"],
            "Keyword": document["Keyword"],
            "Region": document["Region"],
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

print("\nDATA COLLECTION SELESAI")
print(f"Data baru       : {total_insert}")
print(f"Data duplikat   : {total_duplicate}")
print(f"Keyword error   : {total_error}")


# ======================================================
# 7. DATA ANALYTICS
# ======================================================
print("\nMenghitung analisis produk dari Big_Data...")

pipeline_top_produk = [
    {
        "$group": {
            "_id": "$Keyword",
            "produkId": {
                "$first": "$produkId",
            },
            "namaProduk": {
                "$first": "$namaProduk",
            },
            "kategori": {
                "$first": "$Kategori",
            },
            "jumlahDicari": {
                "$sum": "$Minat_Pencarian",
            },
            "rataMinat": {
                "$avg": "$Minat_Pencarian",
            },
            "maxMinat": {
                "$max": "$Minat_Pencarian",
            },
            "totalData": {
                "$sum": 1,
            },
            "tanggalTerakhir": {
                "$max": "$Tanggal",
            },
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

top_produk = list(
    col_big_data.aggregate(
        pipeline_top_produk,
    )
)

total_pencarian = sum(
    item["jumlahDicari"]
    for item in top_produk
)

analytics_docs = []

for index, item in enumerate(top_produk, start=1):
    jumlah_dicari = item["jumlahDicari"]

    persentase = (
        round(
            (jumlah_dicari / total_pencarian) * 100,
            1,
        )
        if total_pencarian > 0
        else 0
    )

    analytics_docs.append(
        {
            "ranking": index,
            "produkId": item.get("produkId"),
            "namaProduk": item.get("namaProduk") or item["_id"],
            "keywordTrend": item["_id"],
            "kategori": item.get("kategori", "-"),
            "jumlahDicari": jumlah_dicari,
            "persentase": persentase,
            "rataMinatTrends": round(item["rataMinat"], 1),
            "maxMinatTrends": item["maxMinat"],
            "totalData": item["totalData"],
            "tanggalTerakhir": item["tanggalTerakhir"],
            "generatedAt": now,
            "sumber": SOURCE,
        }
    )


# ======================================================
# 8. SIMPAN HASIL ANALISIS KE analytics_products
# ======================================================
print("\nMenyimpan hasil analisis ke analytics_products...")

col_analytics.delete_many({})

if analytics_docs:
    col_analytics.insert_many(
        analytics_docs,
    )

    print(
        f"{len(analytics_docs)} data analytics berhasil disimpan."
    )
else:
    print(
        "Tidak ada data analytics yang disimpan."
    )


# ======================================================
# 9. SYNC KE PopularKeyword
# ======================================================
print("\nSinkronisasi PopularKeyword...")

for item in analytics_docs:
    col_popular_keyword.update_one(
        {
            "keyword": item["keywordTrend"],
        },
        {
            "$set": {
                "keyword": item["keywordTrend"],
                "produkId": item["produkId"],
                "namaProduk": item["namaProduk"],
                "jumlahCari": item["jumlahDicari"],
                "ranking": item["ranking"],
                "kategori": item["kategori"],
                "updatedAt": now,
            }
        },
        upsert=True,
    )

print(f"{len(analytics_docs)} keyword populer berhasil disinkronkan.")


# ======================================================
# 10. LAPORAN AKHIR
# ======================================================
print("\n" + "=" * 60)
print("BIG DATA PIPELINE SELESAI")
print(f"Waktu selesai: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 60)

print("\nTop 3 Produk:")
for item in analytics_docs[:3]:
    print(
        f"#{item['ranking']} {item['namaProduk']} "
        f"({item['keywordTrend']}) "
        f"- {item['jumlahDicari']} skor "
        f"({item['persentase']}%)"
    )

print("=" * 60)

client.close()
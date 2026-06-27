# -*- coding: utf-8 -*-
"""
analyze.py
Membaca search_logs dari MongoDB → hitung top produk → simpan ke analytics_products
Dijalankan otomatis oleh GitHub Actions setiap hari pukul 00.00 WIB (17.00 UTC)
"""

import os
from datetime import datetime, timezone
from pymongo import MongoClient, DESCENDING
from dotenv import load_dotenv

load_dotenv()

# ── KONEKSI ───────────────────────────────────────────────────────
MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME   = os.environ.get("DB_APP", "anyaman")

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# ── COLLECTIONS ───────────────────────────────────────────────────
col_search_logs     = db["search_logs"]
col_popular_keyword = db["PopularKeyword"]
col_analytics       = db["analytics_products"]
col_big_data        = db["Big_Data"]

# ── 1. HITUNG TOP PRODUK DARI search_logs ────────────────────────
print("📊 Menghitung top produk dari search_logs ...")

pipeline = [
    {
        "$group": {
            "_id": "$namaProduk",
            "kategori":    {"$first": "$kategori"},
            "jumlahDicari": {"$sum": 1}
        }
    },
    {"$sort": {"jumlahDicari": DESCENDING}},
    {"$limit": 10}
]

top_produk = list(col_search_logs.aggregate(pipeline))
total      = sum(p["jumlahDicari"] for p in top_produk)

print(f"   ✅ Ditemukan {len(top_produk)} produk unik")

# ── 2. HITUNG STATISTIK DARI Big_Data (Google Trends) ────────────
print("📈 Menghitung statistik Google Trends ...")

trends_pipeline = [
    {
        "$group": {
            "_id": "$Keyword",
            "kategori":    {"$first": "$Kategori"},
            "rataMinat":   {"$avg": "$Minat_Pencarian"},
            "maxMinat":    {"$max": "$Minat_Pencarian"},
            "totalData":   {"$sum": 1}
        }
    },
    {"$sort": {"rataMinat": DESCENDING}}
]

trends_stats = list(col_big_data.aggregate(trends_pipeline))
print(f"   ✅ {len(trends_stats)} keyword Google Trends diproses")

# ── 3. SIMPAN KE analytics_products ──────────────────────────────
print("💾 Menyimpan hasil ke analytics_products ...")

now = datetime.now(timezone.utc)

# Hapus hasil analisis lama
col_analytics.delete_many({})

# Insert hasil baru
docs = []

for i, produk in enumerate(top_produk):
    # Cari data Google Trends yang cocok dengan nama produk
    trend_match = next(
        (t for t in trends_stats
         if produk["_id"].lower() in t["_id"].lower()
         or t["_id"].lower() in produk["_id"].lower()),
        None
    )

    docs.append({
        "ranking":          i + 1,
        "namaProduk":       produk["_id"],
        "kategori":         produk.get("kategori", "-"),
        "jumlahDicari":     produk["jumlahDicari"],
        "persentase":       round((produk["jumlahDicari"] / total * 100), 1) if total > 0 else 0,
        "rataMinatTrends":  round(trend_match["rataMinat"], 1) if trend_match else None,
        "maxMinatTrends":   trend_match["maxMinat"] if trend_match else None,
        "generatedAt":      now,
        "sumber":           "search_logs + Google Trends"
    })

if docs:
    col_analytics.insert_many(docs)
    print(f"   ✅ {len(docs)} produk disimpan ke analytics_products")

# ── 4. UPDATE PopularKeyword dari search_logs ─────────────────────
print("🔑 Sync PopularKeyword ...")

keyword_pipeline = [
    {
        "$group": {
            "_id":          "$keyword",
            "jumlahCari":   {"$sum": 1},
            "updatedAt":    {"$max": "$searchedAt"}
        }
    }
]

keywords = list(col_search_logs.aggregate(keyword_pipeline))

for kw in keywords:
    col_popular_keyword.update_one(
        {"keyword": kw["_id"]},
        {"$set": {
            "keyword":    kw["_id"],
            "jumlahCari": kw["jumlahCari"],
            "updatedAt":  now
        }},
        upsert=True
    )

print(f"   ✅ {len(keywords)} keyword diperbarui")

# ── 5. LAPORAN AKHIR ──────────────────────────────────────────────
print("\n" + "=" * 50)
print(f"✅ Analisis selesai: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"📊 Top 3 produk:")
for doc in docs[:3]:
    print(f"   #{doc['ranking']} {doc['namaProduk']} — {doc['jumlahDicari']}x dicari ({doc['persentase']}%)")
print("=" * 50)

client.close()
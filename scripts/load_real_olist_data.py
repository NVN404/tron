#!/usr/bin/env python3
"""
TRON Platform: Real Public Data Loader
Imports authentic Olist Brazilian E-Commerce data into:
- PostgreSQL Source 1 (source_db_1.orders)
- PostgreSQL Source 2 (source_db_2.orders)
- MySQL Source (source_db.sales)
- MongoDB Source (source_db.orders)
And registers real merchants and API keys in Analytics DB.
"""

import csv
import json
import hashlib
import os
import subprocess
import sys
import uuid
from collections import defaultdict, Counter

DATA_DIR = "data/public/olist"
if not os.path.exists(f"{DATA_DIR}/orders.csv"):
    print(f"Error: {DATA_DIR}/orders.csv not found!")
    sys.exit(1)

print("1. Parsing real Olist products...")
products = {}
with open(f"{DATA_DIR}/products.csv", "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        products[row["product_id"]] = row["product_category_name"] or "general"

print("2. Parsing real Olist orders...")
orders_meta = {}
with open(f"{DATA_DIR}/orders.csv", "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        st = row["order_status"].lower()
        if st in ("delivered", "completed"):
            canonical_st = "completed"
        elif st in ("shipped", "invoiced", "processing"):
            canonical_st = "pending"
        elif st == "canceled":
            canonical_st = "cancelled"
        else:
            canonical_st = "refunded"
        
        orders_meta[row["order_id"]] = {
            "status": canonical_st,
            "created_at": row["order_purchase_timestamp"]
        }

print("3. Grouping real items by merchant (seller)...")
seller_items = defaultdict(list)
with open(f"{DATA_DIR}/items.csv", "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        oid = row["order_id"]
        if oid in orders_meta:
            seller_id = row["seller_id"]
            price_minor = int(round(float(row["price"]) * 100))
            seller_items[seller_id].append({
                "order_id": oid,
                "product_id": row["product_id"],
                "category": products.get(row["product_id"], "general"),
                "amount_minor": price_minor,
                "status": orders_meta[oid]["status"],
                "created_at": orders_meta[oid]["created_at"]
            })

# Select top 60 merchants with the highest real sales activity
top_sellers = sorted(seller_items.keys(), key=lambda s: len(seller_items[s]), reverse=True)[:60]
print(f"Selected top {len(top_sellers)} real merchants holding {sum(len(seller_items[s]) for s in top_sellers):,} line items")

# Partition across 4 heterogeneous databases: 15 merchants each
allocations = {
    "pg_source_1": top_sellers[0:15],
    "pg_source_2": top_sellers[15:30],
    "mysql_source": top_sellers[30:45],
    "mongo_source": top_sellers[45:60]
}

os.makedirs("tmp/olist", exist_ok=True)

# Register merchants in Analytics DB
print("4. Registering real merchants & API tokens in Analytics DB...")
tenant_inserts = []
key_inserts = []

for src_id, sellers in allocations.items():
    for s_idx, s in enumerate(sellers):
        t_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{src_id}:{s}"))
        token = f"token-olist-{s[:8]}"
        key_hash = hashlib.sha256(token.encode()).hexdigest()
        
        # Determine dominant product category
        cats = Counter(item["category"] for item in seller_items[s])
        dominant_cat = cats.most_common(1)[0][0] if cats else "retail"
        name = f"Olist Store {s[:8]} ({dominant_cat.replace('_', ' ').title()})"
        
        tenant_inserts.append(f"('{t_uuid}', '{src_id}', '{s}', '{name}', 'healthy')")
        key_inserts.append(f"('{t_uuid}', '{key_hash}', '{token}')")

if tenant_inserts:
    sql = f"""
    INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health)
    VALUES {', '.join(tenant_inserts)}
    ON CONFLICT (tenant_uuid) DO UPDATE SET name = EXCLUDED.name;
    """
    subprocess.run(["docker", "exec", "-i", "mitigata_analytics_db", "psql", "-U", "postgres", "-d", "analytics_db", "-c", sql], check=True)

if key_inserts:
    sql = f"""
    INSERT INTO api_keys (tenant_uuid, key_hash, name)
    VALUES {', '.join(key_inserts)}
    ON CONFLICT (key_hash) DO NOTHING;
    """
    subprocess.run(["docker", "exec", "-i", "mitigata_analytics_db", "psql", "-U", "postgres", "-d", "analytics_db", "-c", sql], check=True)

# 5. Ingest into PG Source 1
print("5. Ingesting real orders into PostgreSQL Source 1...")
pg1_tsv = "tmp/olist/pg1_orders.tsv"
with open(pg1_tsv, "w") as f:
    for s in allocations["pg_source_1"]:
        for item in seller_items[s]:
            order_uuid = str(uuid.uuid4())
            f.write(f"{order_uuid}\t{s}\t{item['product_id']}\t{item['category']}\t{item['amount_minor']}\t{item['status']}\t{item['created_at']}\n")

cmd = "docker exec -i mitigata_source_pg_1 psql -U postgres -d source_db_1 -c \"COPY orders (id, tenant_id, product_id, category, amount_paise, status, created_at) FROM STDIN;\""
with open(pg1_tsv, "r") as f:
    subprocess.run(cmd, shell=True, stdin=f, check=True)

# 6. Ingest into PG Source 2
print("6. Ingesting real orders into PostgreSQL Source 2...")
pg2_tsv = "tmp/olist/pg2_orders.tsv"
with open(pg2_tsv, "w") as f:
    for s in allocations["pg_source_2"]:
        for item in seller_items[s]:
            order_uuid = str(uuid.uuid4())
            f.write(f"{order_uuid}\t{s}\t{item['product_id']}\t{item['category']}\t{item['amount_minor']}\t{item['status']}\t{item['created_at']}\n")

cmd = "docker exec -i mitigata_source_pg_2 psql -U postgres -d source_db_2 -c \"COPY orders (id, tenant_id, product_id, category, amount_paise, status, created_at) FROM STDIN;\""
with open(pg2_tsv, "r") as f:
    subprocess.run(cmd, shell=True, stdin=f, check=True)

# 7. Ingest into MySQL Source
print("7. Ingesting real sales into MySQL Source...")
mysql_sql = "tmp/olist/mysql_sales.sql"
with open(mysql_sql, "w") as f:
    f.write("USE source_db;\n")
    batch = []
    for s in allocations["mysql_source"]:
        for item in seller_items[s]:
            sale_id = "ms_" + uuid.uuid4().hex[:20]
            cat_safe = item['category'].replace("'", "")
            batch.append(f"('{sale_id}', '{s}', '{item['product_id']}', '{cat_safe}', {item['amount_minor']}, '{item['status']}', '{item['created_at']}')")
            if len(batch) >= 1000:
                f.write(f"INSERT INTO sales (sale_id, tenant_id, product_id, category, amount_cents, status, created_at) VALUES {', '.join(batch)};\n")
                batch = []
    if batch:
        f.write(f"INSERT INTO sales (sale_id, tenant_id, product_id, category, amount_cents, status, created_at) VALUES {', '.join(batch)};\n")

cmd = "docker exec -i mitigata_source_mysql mysql -u root -proot < " + mysql_sql
subprocess.run(cmd, shell=True, check=True)

# 8. Ingest into MongoDB Source
print("8. Ingesting real orders into MongoDB Source...")
mongo_json = "tmp/olist/mongo_orders.json"
with open(mongo_json, "w") as f:
    for s in allocations["mongo_source"]:
        for item in seller_items[s]:
            oid = "mg_" + uuid.uuid4().hex[:24]
            doc = {
                "order_id": oid,
                "tenant_id": s,
                "product_id": item["product_id"],
                "category": item["category"],
                "status": item["status"],
                "amount_cents": {"$numberLong": str(item["amount_minor"])},
                "created_at": {"$date": item["created_at"].replace(" ", "T") + "Z"}
            }
            f.write(json.dumps(doc) + "\n")

cmd = f"docker exec -i mitigata_source_mongo mongoimport --db source_db --collection orders < {mongo_json}"
subprocess.run(cmd, shell=True, check=True)

print("\n Real Olist Public Data Ingestion Complete!")
print(f"Total merchants imported: {len(top_sellers)}")
print(f"Top merchant 1: {allocations['pg_source_1'][0]} (token-olist-{allocations['pg_source_1'][0][:8]})")

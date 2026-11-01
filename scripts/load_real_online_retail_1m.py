#!/usr/bin/env python3
"""
TRON Real Data Ingestor: 1,067,371 Real E-Commerce Records (UCI / Kaggle Online Retail II)
Loads 1 Million+ real commercial transactions across 10 multi-tenant retail stores directly
into Source Database and Analytics Database rollups using high-speed streaming COPY.
"""

import sys
import os
import time
import hashlib
import uuid
import datetime
import subprocess
import openpyxl

EXCEL_FILE = "data/public/online_retail/online_retail_II.xlsx"

TENANTS = {
    "United Kingdom": {
        "external_id": "uk_retail_flagship",
        "name": "UK Online Retail Flagship (850k Real Orders)",
        "token": "token-retail-uk",
        "uuid": "00000000-0000-0044-0000-000000000001",
    },
    "EIRE": {
        "external_id": "eire_retail_store",
        "name": "EIRE Ireland Retail (18k Real Orders)",
        "token": "token-retail-ie",
        "uuid": "00000000-0000-0044-0000-000000000002",
    },
    "Germany": {
        "external_id": "germany_retail_store",
        "name": "Germany Direct Retail (17k Real Orders)",
        "token": "token-retail-de",
        "uuid": "00000000-0000-0044-0000-000000000003",
    },
    "France": {
        "external_id": "france_retail_store",
        "name": "France Boutique Retail (14k Real Orders)",
        "token": "token-retail-fr",
        "uuid": "00000000-0000-0044-0000-000000000004",
    },
    "Spain": {
        "external_id": "spain_retail_store",
        "name": "Spain Commercial Retail (4k Real Orders)",
        "token": "token-retail-es",
        "uuid": "00000000-0000-0044-0000-000000000005",
    },
    "Netherlands": {
        "external_id": "netherlands_retail_store",
        "name": "Netherlands Wholesale (5k Real Orders)",
        "token": "token-retail-nl",
        "uuid": "00000000-0000-0044-0000-000000000006",
    },
    "Belgium": {
        "external_id": "belgium_retail_store",
        "name": "Belgium Commerce (3k Real Orders)",
        "token": "token-retail-be",
        "uuid": "00000000-0000-0044-0000-000000000007",
    },
    "Switzerland": {
        "external_id": "switzerland_retail_store",
        "name": "Switzerland Alpine Retail (3k Real Orders)",
        "token": "token-retail-ch",
        "uuid": "00000000-0000-0044-0000-000000000008",
    },
    "Portugal": {
        "external_id": "portugal_retail_store",
        "name": "Portugal Atlantic Retail (2.5k Real Orders)",
        "token": "token-retail-pt",
        "uuid": "00000000-0000-0044-0000-000000000009",
    },
    "Australia": {
        "external_id": "australia_retail_store",
        "name": "Australia Global Retail (2k Real Orders)",
        "token": "token-retail-au",
        "uuid": "00000000-0000-0044-0000-000000000010",
    },
}

DEFAULT_TENANT = TENANTS["United Kingdom"]

def run_psql(db, sql):
    cmd = ["docker", "exec", "-i", db, "psql", "-U", "postgres", "-d", "source_db_1" if "source" in db else "analytics_db"]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate(input=sql)
    if p.returncode != 0:
        print(f"Error executing on {db}: {err}")
    return out

def register_tenants():
    print("Registering 10 real commercial tenants and API keys in Analytics DB...")
    sql_lines = []
    for country, meta in TENANTS.items():
        key_hash = hashlib.sha256(meta["token"].encode("utf-8")).hexdigest()
        sql_lines.append(f"""
        INSERT INTO tenants (tenant_uuid, source_id, external_id, name, health)
        VALUES ('{meta["uuid"]}', 'pg_source_1', '{meta["external_id"]}', '{meta["name"]}', 'healthy')
        ON CONFLICT (tenant_uuid) DO UPDATE SET name = EXCLUDED.name, external_id = EXCLUDED.external_id;

        INSERT INTO api_keys (key_hash, tenant_uuid, name, revoked)
        VALUES ('{key_hash}', '{meta["uuid"]}', '{meta["token"]}', FALSE)
        ON CONFLICT (key_hash) DO NOTHING;
        """)
    run_psql("mitigata_analytics_db", "\n".join(sql_lines))
    print("✓ All 10 retail tenants and SHA-256 Bearer tokens active.")

def process_and_stream():
    t0 = time.time()
    print(f"Opening {EXCEL_FILE}...")
    wb = openpyxl.load_workbook(EXCEL_FILE, read_only=True)
    
    # 1. Pipe to Source Postgres: COPY orders
    source_proc = subprocess.Popen(
        ["docker", "exec", "-i", "mitigata_source_pg_1", "psql", "-U", "postgres", "-d", "source_db_1", "-c",
         "COPY orders (id, tenant_id, product_id, category, amount_paise, status, created_at) FROM STDIN WITH (FORMAT text);"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    hourly_status = {}  # (tenant_uuid, hour, status) -> [order_count, total_amount]
    hourly_product = {} # (tenant_uuid, hour, product_id) -> [units_sold, total_amount]

    total_rows = 0
    valid_rows = 0

    print("Streaming 1,067,371 rows across both annual sheets...")
    for sheetname in wb.sheetnames:
        ws = wb[sheetname]
        rows = ws.iter_rows(values_only=True)
        header = next(rows)
        # Header: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country
        col_inv = header.index("Invoice")
        col_stock = header.index("StockCode")
        col_desc = header.index("Description")
        col_qty = header.index("Quantity")
        col_date = header.index("InvoiceDate")
        col_price = header.index("Price")
        col_country = header.index("Country")

        sheet_count = 0
        for r in rows:
            total_rows += 1
            sheet_count += 1
            
            inv = r[col_inv]
            stock = r[col_stock]
            qty = r[col_qty]
            dt = r[col_date]
            price = r[col_price]
            country = r[col_country]
            desc = r[col_desc]

            if inv is None or stock is None or qty is None or dt is None or price is None:
                continue

            try:
                qty = int(qty)
                price_f = float(price)
            except (ValueError, TypeError):
                continue

            if not isinstance(dt, datetime.datetime):
                continue

            tenant_meta = TENANTS.get(str(country).strip(), DEFAULT_TENANT)
            t_uuid = tenant_meta["uuid"]
            t_ext = tenant_meta["external_id"]

            is_cancelled = qty < 0 or str(inv).strip().startswith("C")
            status = "cancelled" if is_cancelled else "completed"

            unit_price_minor = int(round(price_f * 100))
            order_amount_minor = abs(qty * unit_price_minor)
            units = abs(qty)

            clean_stock = str(stock).strip().replace('\t', '').replace('\n', '')[:30]
            prod_id = f"STOCK-{clean_stock}"
            category = "General Merchandise"
            if desc:
                d_str = str(desc).upper()
                if any(k in d_str for k in ["LIGHT", "LAMP", "CLOCK", "RADIO", "ELECTRONIC"]):
                    category = "Electronics & Lighting"
                elif any(k in d_str for k in ["BAG", "TOTE", "PURSE", "WALLET", "SHIRT"]):
                    category = "Apparel & Accessories"
                elif any(k in d_str for k in ["BOX", "BOWL", "PLATE", "MUG", "CUP", "KITCHEN", "CERAMIC"]):
                    category = "Home & Kitchenware"
                elif any(k in d_str for k in ["CHRISTMAS", "HEART", "GIFT", "DECORATION", "CANDLE"]):
                    category = "Gifts & Seasonal"

            order_id = str(uuid.uuid4())
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S+00")

            # Stream to source DB
            source_proc.stdin.write(f"{order_id}\t{t_uuid}\t{prod_id}\t{category}\t{order_amount_minor}\t{status}\t{dt_str}\n")
            valid_rows += 1

            # Accumulate rollups for Analytics DB
            hour_bucket = dt.replace(minute=0, second=0, microsecond=0)
            
            s_key = (t_uuid, hour_bucket, status)
            if s_key not in hourly_status:
                hourly_status[s_key] = [0, 0]
            hourly_status[s_key][0] += 1
            hourly_status[s_key][1] += order_amount_minor

            if status == "completed":
                p_key = (t_uuid, hour_bucket, prod_id)
                if p_key not in hourly_product:
                    hourly_product[p_key] = [0, 0]
                hourly_product[p_key][0] += units
                hourly_product[p_key][1] += order_amount_minor

            if valid_rows % 100000 == 0:
                print(f"  Processed {valid_rows:,} / 1,067,371 rows ({time.time()-t0:.1f}s)...")

    source_proc.stdin.close()
    source_proc.wait()
    print(f"✓ Source DB populated: {valid_rows:,} real orders written in {time.time()-t0:.1f}s.")

    # 2. Publish rollups to Analytics DB
    print(f"Populating Analytics DB: {len(hourly_status):,} status buckets and {len(hourly_product):,} product buckets...")
    
    analytics_status_proc = subprocess.Popen(
        ["docker", "exec", "-i", "mitigata_analytics_db", "psql", "-U", "postgres", "-d", "analytics_db", "-c",
         "COPY agg_hourly_status (tenant_uuid, source_id, hour, status, order_count, total_amount_minor) FROM STDIN WITH (FORMAT text);"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    for (t_uuid, hr, status), (cnt, amt) in hourly_status.items():
        hr_str = hr.strftime("%Y-%m-%d %H:%M:%S+00")
        analytics_status_proc.stdin.write(f"{t_uuid}\tpg_source_1\t{hr_str}\t{status}\t{cnt}\t{amt}\n")
    analytics_status_proc.stdin.close()
    analytics_status_proc.wait()

    analytics_prod_proc = subprocess.Popen(
        ["docker", "exec", "-i", "mitigata_analytics_db", "psql", "-U", "postgres", "-d", "analytics_db", "-c",
         "COPY agg_hourly_product (tenant_uuid, source_id, hour, product_id, units_sold, gross_revenue_minor) FROM STDIN WITH (FORMAT text);"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    for (t_uuid, hr, prod_id), (units, amt) in hourly_product.items():
        hr_str = hr.strftime("%Y-%m-%d %H:%M:%S+00")
        analytics_prod_proc.stdin.write(f"{t_uuid}\tpg_source_1\t{hr_str}\t{prod_id}\t{units}\t{amt}\n")
    analytics_prod_proc.stdin.close()
    analytics_prod_proc.wait()

    elapsed = time.time() - t0
    print(f"🎉 100% COMPLETE! Ingested {valid_rows:,} real orders into all databases in {elapsed:.1f} seconds!")
    print(f"Speed: {valid_rows/elapsed:,.0f} orders/sec.")

if __name__ == "__main__":
    register_tenants()
    process_and_stream()

import os
import logging
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Konfigurasi database
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "sales_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

CSV_PATH = Path(__file__).parent / "dataset" / "DATASETTechnicalTestDataEngineer.csv"


# Helper
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def _parse_date(val: str):
    """Parse tanggal DD/MM/YYYY → YYYY-MM-DD."""
    try:
        return pd.to_datetime(str(val).strip(), format="%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def _is_scientific_notation(val: str) -> bool:
    """Cek apakah outlet_code berformat scientific notation seperti 3.02E+47."""
    import re
    return bool(re.match(r"^\d+\.?\d*[Ee][+\-]?\d+$", str(val).strip()))


# Extract
def extract(path: Path) -> pd.DataFrame:
    log.info(f"[EXTRACT] Membaca file: {path}")
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    # Rename kolom agar lebih mudah dipakai
    df = df.rename(columns={"sales_period (DD/MM/YYYY)": "sales_period"})
    log.info(f"[EXTRACT] Total baris: {len(df)}")
    return df


# TRANSFORM
def transform(df: pd.DataFrame):
    log.info("[TRANSFORM] Mulai pembersihan data")
    rejected = []

    def reject(row, reason):
        rejected.append({"raw_data": str(row.to_dict()), "reject_reason": reason})

    clean_rows = []

    for _, row in df.iterrows():

        # 1. Cek outlet_code NULL
        if pd.isna(row.get("outlet_code")) or str(row["outlet_code"]).strip() == "":
            reject(row, "outlet_code NULL")
            continue

        # 2. Cek outlet_code scientific notation (tidak bisa dikembalikan)
        if _is_scientific_notation(row["outlet_code"]):
            reject(row, f"outlet_code format scientific notation: {row['outlet_code']}")
            continue

        # 3. Cek outlet_name NULL
        if pd.isna(row.get("outlet_name")) or str(row["outlet_name"]).strip() == "":
            reject(row, "outlet_name NULL")
            continue

        # 4. Cek product_code NULL
        if pd.isna(row.get("product_code")) or str(row["product_code"]).strip() == "":
            reject(row, "product_code NULL")
            continue

        # 5. Cek product_name NULL
        if pd.isna(row.get("product_name")) or str(row["product_name"]).strip() == "":
            reject(row, "product_name NULL")
            continue

        # 6. Cek qty NULL
        if pd.isna(row.get("qty")) or str(row["qty"]).strip() == "":
            reject(row, "qty NULL - tidak bisa menghitung penjualan")
            continue

        # 7. Parse tanggal
        parsed_date = _parse_date(row["sales_period"])
        if not parsed_date:
            reject(row, f"Format tanggal tidak valid: {row['sales_period']}")
            continue

        # 8. Konversi numerik
        try:
            product_code  = str(int(float(row["product_code"])))  # hapus desimal
            qty           = int(float(row["qty"]))
            product_price = float(row["product_price"])
            actual_sales  = float(row["actual_sales"])
        except (ValueError, TypeError) as e:
            reject(row, f"Konversi numerik gagal: {e}")
            continue

        clean_rows.append({
            "sales_period":  parsed_date,
            "outlet_code":   str(row["outlet_code"]).strip(),
            "outlet_name":   str(row["outlet_name"]).strip(),
            "product_code":  product_code,
            "product_name":  str(row["product_name"]).strip(),
            "qty":           qty,
            "product_price": product_price,
            "actual_sales":  actual_sales,
        })

    df_clean    = pd.DataFrame(clean_rows)
    df_rejected = pd.DataFrame(rejected)

    log.info(
        f"[TRANSFORM] Selesai"
        f"Awal: {len(df)} → Bersih: {len(df_clean)} | Ditolak: {len(df_rejected)}"
    )
    if not df_rejected.empty:
        log.warning("[TRANSFORM] Detail baris yang ditolak:")
        for _, r in df_rejected.iterrows():
            log.warning(f"  ✗ {r['reject_reason']}")

    return df_clean, df_rejected


# Load
def run_migrations(conn):
    migration_dir = Path(__file__).parent.parent / "migrations"
    for sql_file in sorted(migration_dir.glob("*.sql")):
        log.info(f"[MIGRATE] {sql_file.name}")
        with conn.cursor() as cur:
            cur.execute(sql_file.read_text())
    conn.commit()
    log.info("[MIGRATE] Semua migrasi selesai")


def load_dimoutlet(cur, df: pd.DataFrame):
    outlets = (
        df[["outlet_code", "outlet_name"]]
        .drop_duplicates("outlet_code")
        .values.tolist()
    )
    execute_values(
        cur,
        """
        INSERT INTO dimoutlet (outlet_code, outlet_name)
        VALUES %s
        ON CONFLICT (outlet_code) DO UPDATE SET outlet_name = EXCLUDED.outlet_name
        """,
        outlets,
    )
    log.info(f"[LOAD] dimoutlet: {len(outlets)} baris")


def load_dimproduct(cur, df: pd.DataFrame):
    products = (
        df[["product_code", "product_name"]]
        .drop_duplicates("product_code")
        .values.tolist()
    )
    execute_values(
        cur,
        """
        INSERT INTO dimproduct (product_code, product_name)
        VALUES %s
        ON CONFLICT (product_code) DO UPDATE SET product_name = EXCLUDED.product_name
        """,
        products,
    )
    log.info(f"[LOAD] dimproduct: {len(products)} baris")


def load_factsales(cur, df: pd.DataFrame):
    cur.execute("SELECT outlet_code, outlet_id FROM dimoutlet")
    outlet_map = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT product_code, product_id FROM dimproduct")
    product_map = {r[0]: r[1] for r in cur.fetchall()}

    records = [
        (
            row["sales_period"],
            outlet_map[row["outlet_code"]],
            product_map[row["product_code"]],
            row["qty"],
            row["product_price"],
            row["actual_sales"],
        )
        for _, row in df.iterrows()
    ]

    execute_values(
        cur,
        """
        INSERT INTO factsales
            (sales_period, outlet_id, product_id, qty, product_price, actual_sales)
        VALUES %s
        """,
        records,
    )
    log.info(f"[LOAD] factsales: {len(records)} baris")


def load_rejected(cur, df_rejected: pd.DataFrame):
    if df_rejected.empty:
        return
    records = df_rejected[["raw_data", "reject_reason"]].values.tolist()
    execute_values(
        cur,
        "INSERT INTO pipelinerejectedrows (raw_data, reject_reason) VALUES %s",
        records,
    )
    log.info(f"[LOAD] pipelinerejectedrows: {len(records)} baris ditolak dicatat")


def load(df: pd.DataFrame, df_rejected: pd.DataFrame):
    conn = get_connection()
    try:
        run_migrations(conn)
        with conn.cursor() as cur:
            load_dimoutlet(cur, df)
            load_dimproduct(cur, df)
            load_factsales(cur, df)
            load_rejected(cur, df_rejected)
        conn.commit()
        log.info("[LOAD] Semua data berhasil dimuat.")
    except Exception as e:
        conn.rollback()
        log.error(f"[LOAD] Gagal! Rollback. Error: {e}")
        raise
    finally:
        conn.close()


# Main
def main():
    log.info("=" * 55)
    log.info("SALES DATA PIPELINE - MULAI")
    log.info("=" * 55)
    df_raw            = extract(CSV_PATH)
    df_clean, df_rej  = transform(df_raw)
    load(df_clean, df_rej)
    log.info("=" * 55)
    log.info("PIPELINE SELESAI SUKSES")
    log.info("=" * 55)


if __name__ == "__main__":

    main()

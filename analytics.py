import os
import logging
from pathlib import Path

import pandas as pd
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "sales_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    # Monthly sales per outlet
    sql = """
        SELECT
            TO_CHAR(fs.sales_period, 'YYYYMM') AS "Period",
            do2.outlet_code                     AS "Outlet Code",
            do2.outlet_name                     AS "Outlet Name",
            SUM(fs.actual_sales)                AS "Sales"
        FROM factsales fs
        JOIN dimoutlet do2 ON fs.outlet_id = do2.outlet_id
        GROUP BY
            TO_CHAR(fs.sales_period, 'YYYYMM'),
            do2.outlet_code,
            do2.outlet_name
        ORDER BY "Period", "Outlet Code"
    """

    df = pd.read_sql_query(sql, conn)
    conn.close()

    print("\n===== MONTHLY SALES PER OUTLET =====")
    print(df.to_string(index=False))

    # Simpan ke Excel
    output = Path(__file__).parent.parent / "dataset" / "monthlysalesreport.xlsx"
    df.to_excel(output, index=False)
    log.info(f"Laporan disimpan: {output}")


if __name__ == "__main__":
    main()
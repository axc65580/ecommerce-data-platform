import duckdb
import pandas as pd
import os

DB_PATH = 'C:/Users/ahaly/ecommerce-data-platform/ecommerce.duckdb'

print("Connecting to DuckDB...")
conn = duckdb.connect(DB_PATH)

# Install and load parquet extension
conn.execute("INSTALL parquet")
conn.execute("LOAD parquet")

tables = {
    'revenue_by_product': 'C:/Users/ahaly/ecommerce-data-platform/data/gold/revenue_by_product',
    'user_summary': 'C:/Users/ahaly/ecommerce-data-platform/data/gold/user_summary',
    'conversion_funnel': 'C:/Users/ahaly/ecommerce-data-platform/data/gold/conversion_funnel',
}

for table_name, path in tables.items():
    print("Loading " + table_name + "...")
    parquet_glob = path.replace("\\", "/") + "/*.parquet"
    conn.execute("DROP TABLE IF EXISTS " + table_name)
    conn.execute("CREATE TABLE " + table_name + " AS SELECT * FROM read_parquet('" + parquet_glob + "')")
    count = conn.execute("SELECT COUNT(*) FROM " + table_name).fetchone()[0]
    print("Loaded " + str(count) + " rows into " + table_name)

conn.close()
print("\nAll tables loaded into DuckDB successfully!")

import duckdb
import json
from datetime import datetime

DB_PATH = 'C:/Users/ahaly/ecommerce-data-platform/ecommerce.duckdb'
conn = duckdb.connect(DB_PATH)

query = "SELECT MIN(CASE WHEN event_type = 'PURCHASE' THEN event_count END) < MAX(CASE WHEN event_type = 'PAGE_VIEW' THEN event_count END) FROM conversion_funnel"
result = conn.execute(query).fetchone()[0]
status = 'PASS' if result == True else 'FAIL'
print(status + ' | purchases are less than page views | result: ' + str(result))
conn.close()

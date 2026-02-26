import duckdb
import json
from datetime import datetime

DB_PATH = 'C:/Users/ahaly/ecommerce-data-platform/ecommerce.duckdb'

conn = duckdb.connect(DB_PATH)

results = []
passed = 0
failed = 0

def check(name, query, expectation):
    global passed, failed
    result = conn.execute(query).fetchone()[0]
    status = 'PASS' if expectation(result) else 'FAIL'
    if status == 'PASS':
        passed += 1
    else:
        failed += 1
    results.append({
        'check': name,
        'result': str(result),
        'status': status,
        'timestamp': datetime.now().isoformat()
    })
    print(status + ' | ' + name + ' | result: ' + str(result))

print('Running Data Quality Checks...\n')

check(
    'revenue_by_product has 8 products',
    'SELECT COUNT(*) FROM revenue_by_product',
    lambda x: x == 8
)

check(
    'no negative revenue',
    'SELECT COUNT(*) FROM revenue_by_product WHERE total_revenue < 0',
    lambda x: x == 0
)

check(
    'all products have orders',
    'SELECT COUNT(*) FROM revenue_by_product WHERE total_orders = 0',
    lambda x: x == 0
)

check(
    'price buckets are valid',
    "SELECT COUNT(*) FROM revenue_by_product WHERE price_bucket NOT IN ('budget', 'mid_range', 'premium')",
    lambda x: x == 0
)

check(
    'no users with negative spend',
    'SELECT COUNT(*) FROM user_summary WHERE total_spent < 0',
    lambda x: x == 0
)

check(
    'user_id is never null',
    'SELECT COUNT(*) FROM user_summary WHERE user_id IS NULL',
    lambda x: x == 0
)

check(
    'total purchases never negative',
    'SELECT COUNT(*) FROM user_summary WHERE total_purchases < 0',
    lambda x: x == 0
)

check(
    'funnel has exactly 4 event types',
    'SELECT COUNT(*) FROM conversion_funnel',
    lambda x: x == 4
)

check(
    'page views are the most common event',
    'SELECT event_type FROM conversion_funnel ORDER BY event_count DESC LIMIT 1',
    lambda x: x == 'PAGE_VIEW'
)

check(
    'purchases are less than page views',
    'SELECT MIN(CASE WHEN event_type = 'PURCHASE' THEN event_count END) < MAX(CASE WHEN event_type = 'PAGE_VIEW' THEN event_count END) FROM conversion_funnel',
    lambda x: x == True
)

print('\n' + '='*50)
print('Data Quality Report')
print('='*50)
print('Total checks : ' + str(passed + failed))
print('Passed       : ' + str(passed))
print('Failed       : ' + str(failed))
print('='*50)

report_path = 'great_expectations/dq_report.json'
with open(report_path, 'w') as f:
    json.dump({
        'run_timestamp': datetime.now().isoformat(),
        'total': passed + failed,
        'passed': passed,
        'failed': failed,
        'checks': results
    }, f, indent=2)

print('Report saved to: ' + report_path)
conn.close()

if failed > 0:
    raise Exception(str(failed) + ' data quality checks failed!')

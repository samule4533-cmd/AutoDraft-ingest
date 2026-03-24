import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect('postgresql://autodraft:autodraft@localhost:5433/autodraft')
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== 문서 처리 상태 ===")
cur.execute("SELECT file_id, file_name, status, chunk_count, error_message, updated_at FROM document_state ORDER BY updated_at DESC")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r['file_name']} | {r['status']} | 청크:{r['chunk_count']} | {r['updated_at']}")
        if r['error_message']:
            print(f"    오류: {r['error_message']}")
else:
    print("  (없음)")

print("\n=== 최근 처리 로그 ===")
cur.execute("SELECT file_id, event, detail, created_at FROM ingest_log ORDER BY created_at DESC LIMIT 20")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r['event']:12} | {r['detail'] or ''} | {r['created_at']}")
else:
    print("  (없음)")

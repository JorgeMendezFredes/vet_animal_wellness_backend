import asyncio
import os
import sys
from collections import Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.supabase import get_supabase

async def debug_states():
    supabase = get_supabase()
    all_rows = []
    start = 0
    batch_size = 1000
    
    while True:
        response = supabase.table("comprobantes")\
            .select("estado")\
            .eq("is_active", True)\
            .range(start, start + batch_size - 1)\
            .execute()
        
        data = response.data
        if not data:
            break
        all_rows.extend(data)
        if len(data) < batch_size:
            break
        start += batch_size
        
    states = [r.get('estado') for r in all_rows]
    normalized_states = [s.strip().upper() if s else 'NONE' for s in states]
    
    print(f"Total rows fetched (is_active=True): {len(all_rows)}")
    print("\nOriginal States Distribution:")
    for state, count in Counter(states).items():
        print(f"'{state}': {count}")
        
    print("\nNormalized States Distribution:")
    for state, count in Counter(normalized_states).items():
        print(f"'{state}': {count}")

if __name__ == "__main__":
    asyncio.run(debug_states())

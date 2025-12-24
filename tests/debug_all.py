import asyncio
import os
import sys
from collections import Counter

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.supabase import get_supabase

async def debug_all_states():
    supabase = get_supabase()
    all_rows = []
    start = 0
    batch_size = 1000
    
    while True:
        response = supabase.table("comprobantes")\
            .select("estado, is_active, version, comprobante")\
            .range(start, start + batch_size - 1)\
            .execute()
        
        data = response.data
        if not data:
            break
        all_rows.extend(data)
        if len(data) < batch_size:
            break
        start += batch_size
        
    print(f"Total rows fetched (ALL): {len(all_rows)}")
    
    active_rows = [r for r in all_rows if r['is_active']]
    inactive_rows = [r for r in all_rows if not r['is_active']]
    
    print(f"Active rows: {len(active_rows)}")
    print(f"Inactive rows: {len(inactive_rows)}")
    
    valid_active = [r for r in active_rows if r.get('estado', '').strip().upper() != 'ANULADO']
    print(f"Valid Active (!= ANULADO): {len(valid_active)}")
    
    print("\nActive States Distribution:")
    states = [r.get('estado') for r in active_rows]
    for state, count in Counter(states).items():
        print(f"'{state}': {count}")

    # Check for PENDIENTE in inactive
    inactive_pendientes = [r for r in inactive_rows if r.get('estado', '').strip().upper() == 'PENDIENTE']
    print(f"\nInactive PENDIENTE rows: {len(inactive_pendientes)}")

if __name__ == "__main__":
    asyncio.run(debug_all_states())

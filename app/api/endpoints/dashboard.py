from fastapi import APIRouter, HTTPException
from app.db.supabase import get_supabase
import pandas as pd

router = APIRouter()

@router.get("/stats")
def get_dashboard_stats():
    """
    Get basic dashboard statistics from 'comprobantes' table.
    """
    supabase = get_supabase()
    
    # Fetch all data effectively (handling default 1000 row limit)
    all_rows = []
    chunk_size = 1000
    current_start = 0
    
    while True:
        response = supabase.table('comprobantes').select("*").range(current_start, current_start + chunk_size - 1).execute()
        rows = response.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < chunk_size:
            break
        current_start += chunk_size
        
        # Safety break for massive datasets to avoid timeout, though 100k is fine given logic
        if len(all_rows) > 50000: 
            break
    
    if not all_rows:
        return {"message": "No data found"}
        
    df = pd.DataFrame(all_rows)
    
    # Calculate advanced analytics
    from app.services.analytics import calculate_analytics
    stats = calculate_analytics(df)

    return stats

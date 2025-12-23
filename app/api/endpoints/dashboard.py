from fastapi import APIRouter, HTTPException
from app.db.supabase import get_supabase
from app.services import analytics
import pandas as pd
import time

router = APIRouter()

# Simple in-memory cache
_df_cache = None
_last_cache_time = 0

def fetch_all_comprobantes():
    supabase = get_supabase()
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
        
        # Safety break
        if len(all_rows) > 50000: 
            break
            
    return pd.DataFrame(all_rows)

@router.get("/stats")
def get_dashboard_stats(
    year: int = 0, 
    month: int = 0, 
    status: str = 'all', 
    tipo: str = 'all', 
    search: str = ''
):
    """
    Returns aggregated KPIs for the dashboard.
    Uses in-memory caching and applies server-side filters.
    """
    global _df_cache, _last_cache_time
    
    current_time = time.time()
    
    if _df_cache is None or (current_time - _last_cache_time) > 600:
        try:
            _df_cache = fetch_all_comprobantes()
            _last_cache_time = current_time
        except Exception as e:
            if _df_cache is None: _df_cache = pd.DataFrame()

    if _df_cache.empty:
        return {"error": "No data available"}

    # Prepare filters dict
    filters = {
        "year": year,
        "month": month,
        "status": status,
        "tipo": tipo,
        "search": search
    }

    return analytics.calculate_analytics(_df_cache, filters=filters)

@router.get("/client_search")
def search_client_endpoint(query: str):
    """
    Endpoint to search for a specific client's history.
    """
    global _df_cache
    if _df_cache is None or _df_cache.empty:
        # Try to load if empty
        try:
             _df_cache = fetch_all_comprobantes()
        except:
             return []

    return analytics.search_client_history(_df_cache, query)

from fastapi import APIRouter, HTTPException, Query
from app.db.supabase import get_supabase
from app.services import analytics
from app.schemas.dashboard import DashboardSummaryResponse, DashboardInsightsResponse, DashboardTransactionsResponse
import pandas as pd
import time
import traceback

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
        if len(all_rows) > 50000: 
            break
            
    return pd.DataFrame(all_rows)

def get_cached_df():
    global _df_cache, _last_cache_time
    current_time = time.time()
    
    if _df_cache is None or (current_time - _last_cache_time) > 600:
        try:
            print("🔄 Refreshing cache from Supabase...")
            _df_cache = fetch_all_comprobantes()
            _last_cache_time = current_time
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            if _df_cache is None: raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")
    
    if _df_cache is None or _df_cache.empty:
        raise HTTPException(status_code=404, detail="No data available")
        
    return _df_cache

# --- NEW MODULAR ENDPOINTS ---

@router.get("/stats/summary", response_model=DashboardSummaryResponse)
def get_dashboard_summary(
    year: int = 0, 
    month: int = 0, 
    status: str = 'all', 
    tipo: str = 'all', 
    search: str = ''
):
    """
    Lightweight endpoint. Returns critical KPIs, aggregated trends, and seasonality.
    Fast load time.
    """
    try:
        df_raw = get_cached_df()
        
        # Preprocess full dataset once
        df = analytics.preprocess_df(df_raw)
        
        # Create full copy for historical trends (unfiltered by year/month)
        df_full = df.copy() 
        
        # Apply filters for current view
        filters = {"year": year, "month": month, "status": status, "tipo": tipo, "search": search}
        df_filtered = analytics.apply_filters(df, filters)
        
        return analytics.get_kpis_summary(df_filtered, df_full)
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/insights", response_model=DashboardInsightsResponse)
def get_dashboard_insights(
    year: int = 0, 
    month: int = 0, 
    status: str = 'all', 
    tipo: str = 'all', 
    search: str = ''
):
    """
    Secondary endpoint. Returns Customer Insights, Payment Mix, Data Quality, Aging.
    Can be loaded lazily.
    """
    try:
        df_raw = get_cached_df()
        df = analytics.preprocess_df(df_raw)
        df_full = df.copy() # Need full for quality metrics
        
        filters = {"year": year, "month": month, "status": status, "tipo": tipo, "search": search}
        df_filtered = analytics.apply_filters(df, filters)
        
        return analytics.get_insights(df_filtered, df_full)
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/transactions", response_model=DashboardTransactionsResponse)
def get_dashboard_transactions(
    year: int = 0, 
    month: int = 0, 
    status: str = 'all', 
    tipo: str = 'all', 
    search: str = ''
):
    """
    Heavy endpoint. Returns detailed lists (Drilldown, Pending Invoices).
    Should be loaded only when needed.
    """
    try:
        df_raw = get_cached_df()
        df = analytics.preprocess_df(df_raw)
        
        filters = {"year": year, "month": month, "status": status, "tipo": tipo, "search": search}
        df_filtered = analytics.apply_filters(df, filters)
        
        return analytics.get_transactions(df_filtered)
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/client_search")
def search_client_endpoint(query: str):
    try:
        df_raw = get_cached_df()
        return analytics.search_client_history(df_raw, query)
    except Exception as e:
        return []

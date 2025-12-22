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
    
    # Fetch data (limiting to recent or all for now, depending on volume)
    # For now, let's just get a count and sum of 'facturado' to verify connection
    response = supabase.table('comprobantes').select("*").execute()
    
    if not response.data:
        return {"message": "No data found"}
        
    df = pd.DataFrame(response.data)
    
    # Calculate advanced analytics
    from app.services.analytics import calculate_analytics
    stats = calculate_analytics(df)

    return stats

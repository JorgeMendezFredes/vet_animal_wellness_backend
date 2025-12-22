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
    response = supabase.table('comprobantes').select("facturado, pendiente, estado").execute()
    
    if not response.data:
        return {"message": "No data found"}
        
    df = pd.DataFrame(response.data)
    
    total_facturado = float(df['facturado'].sum())
    total_pendiente = float(df['pendiente'].sum())
    count_comprobantes = int(len(df))
    
    # Simple grouping by status
    status_counts = df['estado'].value_counts().to_dict()
    # Convert keys and values to standard types if needed
    status_counts = {k: int(v) for k, v in status_counts.items()}

    return {
        "total_facturado": total_facturado,
        "total_pendiente": total_pendiente,
        "count_comprobantes": count_comprobantes,
        "status_distribution": status_counts
    }

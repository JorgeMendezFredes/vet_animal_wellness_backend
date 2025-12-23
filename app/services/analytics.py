import pandas as pd
import numpy as np
from datetime import datetime

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and prepares the dataframe for analytics.
    Guarantees required columns exist.
    """
    if df.empty:
        return df
        
    df = df.copy()
    
    # Required columns and default types
    numeric_cols = ['facturado', 'pagado', 'pendiente', 'descuento']
    
    def clean_currency(val):
        if pd.isna(val) or val == '': return 0.0
        if isinstance(val, (int, float)): return float(val)
        s = str(val).replace('$', '').replace(' ', '').replace('.', '').replace(',', '.')
        try: return float(s)
        except: return 0.0

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_currency)
        else:
            df[col] = 0.0
    
    # Ensure fecha_emision is datetime
    if 'fecha_emision' in df.columns:
        df['fecha_emision'] = pd.to_datetime(df['fecha_emision'], errors='coerce')
        # Fix: Ensure timezone-naive to avoid "Cannot subtract tz-naive and tz-aware" errors
        if df['fecha_emision'].dt.tz is not None:
            df['fecha_emision'] = df['fecha_emision'].dt.tz_localize(None)
        df = df.dropna(subset=['fecha_emision'])
    else:
        # Fallback if column is totally missing
        df['fecha_emision'] = pd.Timestamp.now()

    df['year'] = df['fecha_emision'].dt.year
    df['month'] = df['fecha_emision'].dt.month
    df['day_name'] = df['fecha_emision'].dt.day_name()
    df['hour'] = df['fecha_emision'].dt.hour
    
    # Column safety
    if 'estado' not in df.columns: df['estado'] = 'VIGENTE'
    if 'cliente' not in df.columns: df['cliente'] = 'Desconocido'
    if 'comprobante' not in df.columns: df['comprobante'] = 'S/N'
    if 'tipo' not in df.columns: df['tipo'] = 'Venta' # Default type
    
    # Payment Mix
    if 'forma_pago_raw' in df.columns:
        def categorize_payment(pago):
            p = str(pago).lower()
            if any(x in p for x in ['tarjeta', 'transbank', 'tbk']): return 'Tarjeta/POS'
            if 'transferencia' in p: return 'Transferencia'
            if 'efectivo' in p: return 'Efectivo'
            if 'sin boleta' in p: return 'Sin Boleta'
            return 'Otros'
        df['payment_type'] = df['forma_pago_raw'].fillna('Otros').apply(categorize_payment)
    else:
        df['payment_type'] = 'Otros'
    
    df['has_discount'] = df['descuento'] > 0
    return df

def calculate_analytics(df: pd.DataFrame, filters: dict = None):
    """
    Calculates comprehensive analytics. 
    Applies filters server-side if provided.
    """
    if df.empty:
        return {}

    # 1. Preprocess
    df = preprocess_df(df)
    if df.empty: return {}
    
    # Save full copy for global trends
    df_full = df.copy()

    # 2. APPLY FILTERS
    if filters:
        if filters.get('year') and filters['year'] != 0:
            df = df[df['year'] == filters['year']]
        if filters.get('month') and filters['month'] != 0:
            df = df[df['month'] == filters['month']]
        if filters.get('status') and filters['status'] != 'all':
            df = df[df['estado'] == filters['status']]
        if filters.get('tipo') and filters['tipo'] != 'all':
            df = df[df['tipo'] == filters['tipo']]
        if filters.get('search'):
            q = str(filters['search']).lower().strip()
            df = df[df['cliente'].astype(str).str.lower().str.contains(q, na=False)]

    # --- 1. Filtered KPI Summary ---
    df_valid = df[df['estado'] != 'ANULADO'].copy()
    
    summary_kpis = {
        "facturado": float(df_valid['facturado'].sum()),
        "pagado": float(df_valid['pagado'].sum()),
        "pendiente": float(df_valid['pendiente'].sum()),
        "descuento": float(df_valid['descuento'].sum()),
        "tx_count": int(len(df_valid)),
        "avg_ticket": float(df_valid['facturado'].mean()) if not df_valid.empty else 0.0,
        "anuladas_count": int(len(df[df['estado'] == 'ANULADO'])),
        "anuladas_percent": float(len(df[df['estado'] == 'ANULADO']) / len(df) * 100) if len(df) > 0 else 0.0,
        "discount_percent": float(df['descuento'].sum() / (df['facturado'].sum() + df['descuento'].sum()) * 100) if (df['facturado'].sum() + df['descuento'].sum()) > 0 else 0.0
    }

    # --- 2. Historical Trends (Using df_full) ---
    group_dims_month = ['year', 'month', 'estado', 'tipo']
    monthly_stats_full = df_full.groupby(group_dims_month).agg({
        'facturado': 'sum', 'pagado': 'sum', 'pendiente': 'sum', 'descuento': 'sum', 'fecha_emision': 'count', 'has_discount': 'sum'
    }).reset_index()

    monthly_seasonality = []
    for _, r in monthly_stats_full.iterrows():
        monthly_seasonality.append({
            "year": int(r['year']), "month": int(r['month']), "estado": str(r['estado']),
            "facturado": float(r['facturado']), "pagado": float(r['pagado']),
            "pendiente": float(r['pendiente']), "descuento": float(r['descuento']),
            "tx_count": int(r['fecha_emision']), "count_with_discount": int(r['has_discount']),
            "tipo": str(r['tipo'])
        })

    # Weekly/Yearly aggregation for kpis_by_year (Required by frontend)
    group_dims_year = ['year', 'estado', 'tipo']
    yearly_stats = df_full.groupby(group_dims_year).agg({
        'facturado': 'sum', 'pagado': 'sum', 'pendiente': 'sum', 'descuento': 'sum', 'fecha_emision': 'count'
    }).reset_index()

    kpis_by_year = []
    for _, r in yearly_stats.iterrows():
        kpis_by_year.append({
            "year": int(r['year']),
            "estado": str(r['estado']),
            "tipo": str(r['tipo']),
            "facturado": float(r['facturado']),
            "pagado": float(r['pagado']),
            "pendiente": float(r['pendiente']),
            "descuento": float(r['descuento']),
            "tx_count": int(r['fecha_emision'])
        })


    # --- 3. Operational Analysis (DoW, Heatmap, Daily) ---
    dow_map = {0:'Lunes', 1:'Martes', 2:'Miércoles', 3:'Jueves', 4:'Viernes', 5:'Sábado', 6:'Domingo'}
    
    # DoW
    df_valid['dow_idx'] = df_valid['fecha_emision'].dt.dayofweek
    dow_stats = df_valid.groupby('dow_idx').agg(f=('facturado', 'sum'), c=('fecha_emision', 'count'), d=('fecha_emision', lambda x: x.dt.date.nunique())).reset_index()
    dow_analysis = [{"day": dow_map[int(r['dow_idx'])], "facturado": float(r['f']), "tx_count": int(r['c']), "avg_daily_sales": float(r['f']/r['d']) if r['d']>0 else 0.0} for _, r in dow_stats.iterrows()]

    # Heatmap
    heatmap_stats = df_valid.groupby(['dow_idx', 'hour']).agg(f=('facturado', 'sum'), c=('fecha_emision', 'count')).reset_index()
    demanda_heatmap = [{"day": dow_map[int(r['dow_idx'])], "hour": int(r['hour']), "facturado": float(r['f']), "tx_count": int(r['c'])} for _, r in heatmap_stats.iterrows()]

    # Daily Trends
    daily_stats = df_valid.groupby(df_valid['fecha_emision'].dt.date).agg(f=('facturado', 'sum'), c=('fecha_emision', 'count')).reset_index()
    daily_trends = [{"date": str(r['fecha_emision']), "facturado": float(r['f']), "tx_count": int(r['c'])} for _, r in daily_stats.iterrows()]

    # --- 4. Payment Mix ---
    payment_stats = df.groupby(['year', 'month', 'payment_type']).agg(f=('facturado', 'sum'), c=('fecha_emision', 'count')).reset_index()
    payment_mix_data = [{"year": int(r['year']), "month": int(r['month']), "type": str(r['payment_type']), "amount": float(r['f']), "count": int(r['c'])} for _, r in payment_stats.iterrows()]

    # --- 5. Customer Insights ---
    customer_stats = df_valid.groupby('cliente').agg(f=('facturado', 'sum'), c=('fecha_emision', 'count')).reset_index()
    total_clients = len(customer_stats)
    top_20_rev = customer_stats.sort_values('f', ascending=False).head(20)['f'].sum()
    total_rev = df_valid['facturado'].sum()
    
    customer_insights = {
        "total_clients": total_clients, "retention_rate": float(len(customer_stats[customer_stats['c']>1])/total_clients*100) if total_clients>0 else 0.0,
        "pareto_share": float(top_20_rev/total_rev*100) if total_rev>0 else 0.0,
        "top_20_clients": [{"cliente": str(r['cliente']), "facturado": float(r['f']), "tx_count": int(r['c'])} for _, r in customer_stats.sort_values('f', ascending=False).head(20).iterrows()]
    }

    # --- 6. Aging & Quality ---
    aging_data = []
    df_pending = df_valid[df_valid['pendiente'] > 0.0].copy()
    if not df_pending.empty:
        ref_date = df_full['fecha_emision'].max()
        df_pending['days_since'] = (ref_date - df_pending['fecha_emision']).dt.days
        bins = {"0-7 días": df_pending[df_pending['days_since']<=7]['pendiente'].sum(), "8-30 días": df_pending[(df_pending['days_since']>7)&(df_pending['days_since']<=30)]['pendiente'].sum(), "31-60 días": df_pending[(df_pending['days_since']>30)&(df_pending['days_since']<=60)]['pendiente'].sum(), "60+ días": df_pending[df_pending['days_since']>60]['pendiente'].sum()}
        aging_data = [{"range": k, "amount": float(v)} for k, v in bins.items()]

    quality = {
        "total_records": len(df), "missing_payment_pct": float(df_full['forma_pago_raw'].isna().sum()/len(df_full)*100) if 'forma_pago_raw' in df_full.columns else 0.0,
        "missing_client_pct": float(df_full['cliente'].isna().sum()/len(df_full)*100) if len(df_full)>0 else 0.0,
        "anuladas_pct": float(len(df_full[df_full['estado']=='ANULADO'])/len(df_full)*100) if len(df_full)>0 else 0.0
    }

    # --- 7. Tables ---
    drilldown = df.sort_values('fecha_emision', ascending=False).head(5000)
    cols_to_dict = ['fecha_emision', 'comprobante', 'cliente', 'facturado', 'pagado', 'pendiente', 'descuento', 'estado', 'payment_type']
    drilldown_data = drilldown[cols_to_dict].copy()
    drilldown_data['fecha_emision'] = drilldown_data['fecha_emision'].dt.strftime('%Y-%m-%d %H:%M')
    drilldown_dict = drilldown_data.fillna('').to_dict('records')

    pending_invoices = []
    if not df_pending.empty:
        current_time = pd.Timestamp.now()
        for _, r in df_pending.sort_values('fecha_emision', ascending=True).head(2000).iterrows():
            pending_invoices.append({"fecha_emision": r['fecha_emision'].strftime('%Y-%m-%d'), "comprobante": str(r['comprobante']), "cliente": str(r['cliente']), "pendiente": float(r['pendiente']), "facturado": float(r['facturado']), "days_overdue": int((current_time - r['fecha_emision']).days)})

    result = {
        "summary": summary_kpis, "kpis_by_year": kpis_by_year, "monthly_seasonality": monthly_seasonality, "dow_analysis": dow_analysis, "daily_trends": daily_trends, "demanda_heatmap": demanda_heatmap,
        "payment_mix_data": payment_mix_data, "customer_insights": customer_insights, "aging_analysis": aging_data, "data_quality": quality, "drilldown_data": drilldown_dict,
        "pending_invoices": pending_invoices,
        "discounts_analysis": {
            "avg_ticket_with_discount": float(df_valid[df_valid['has_discount']]['facturado'].mean()) if not df_valid[df_valid['has_discount']].empty else 0.0,
            "avg_ticket_no_discount": float(df_valid[~df_valid['has_discount']]['facturado'].mean()) if not df_valid[~df_valid['has_discount']].empty else 0.0,
            "top_discounted_clients": [{"cliente": str(r['cliente']), "descuento": float(r['descuento'])} for _, r in df_valid.groupby('cliente')['descuento'].sum().reset_index().sort_values('descuento', ascending=False).head(10).iterrows()]
        },
        "anuladas_audit": df[ (df['estado']=='ANULADO') & (df['pagado']>0) ][['fecha_emision', 'comprobante', 'cliente', 'facturado', 'pagado', 'estado']].copy().fillna(0).to_dict('records')
    }
    for r in result['anuladas_audit']: r['fecha_emision'] = r['fecha_emision'].strftime('%Y-%m-%d')

    def sanitize(obj):
        if isinstance(obj, dict): return {k: sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [sanitize(i) for i in obj]
        elif isinstance(obj, float):
            if np.isnan(obj) or np.isinf(obj): return 0.0
            return obj
        return obj
    return sanitize(result)

def search_client_history(df: pd.DataFrame, query: str):
    if df.empty or not query: return []
    df = preprocess_df(df)
    query = str(query).lower().strip()
    mask = df['cliente'].str.lower().str.contains(query, regex=False)
    client_df = df[mask].sort_values(by='fecha_emision', ascending=False)
    results = []
    for _, r in client_df.iterrows():
        results.append({"fecha_emision": r['fecha_emision'].strftime('%Y-%m-%d %H:%M'), "comprobante": str(r['comprobante']), "cliente": str(r['cliente']), "facturado": float(r['facturado']), "pagado": float(r['pagado']), "pendiente": float(r['pendiente']), "descuento": float(r['descuento']), "estado": str(r['estado']), "payment_type": str(r['payment_type']), "tipo": str(r['tipo'])})
    return results

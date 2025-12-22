import pandas as pd
import numpy as np
from datetime import datetime

def calculate_analytics(df: pd.DataFrame):
    """
    Calculates comprehensive analytics from the comprobantes dataframe.
    """
    if df.empty:
        return {}

    # --- Preprocessing ---
    # Convert numeric columns safely
    # --- Preprocessing ---
    # Convert numeric columns safely (handling various formats like "$1.000,00" or int)
    numeric_cols = ['facturado', 'pagado', 'pendiente', 'descuento']
    
    def clean_currency(val):
        if pd.isna(val) or val == '':
            return 0
        if isinstance(val, (int, float)):
            return val
        s = str(val)
        # Remove symbols and thousands separators
        s = s.replace('$', '').replace(' ', '').replace('.', '')
        # Replace decimal comma with dot
        s = s.replace(',', '.')
        try:
            return float(s)
        except ValueError:
            return 0

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_currency)
        else:
            df[col] = 0

    # Convert date
    df['fecha_emision'] = pd.to_datetime(df['fecha_emision'], errors='coerce')
    df['year'] = df['fecha_emision'].dt.year
    df['month'] = df['fecha_emision'].dt.month
    df['day_name'] = df['fecha_emision'].dt.day_name() # English names by default, can map later
    df['hour'] = df['fecha_emision'].dt.hour
    
    # Filter out invalid dates if any
    df = df.dropna(subset=['fecha_emision'])

    # --- 1. KPIs per Year, Status & Tipo ---
    # Ensure columns exist
    if 'estado' not in df.columns:
        df['estado'] = 'VIGENTE'
    
    # Identify if 'tipo' exists (e.g. Boleta, Factura)
    has_tipo = 'tipo' in df.columns
    group_dims_year = ['year', 'estado']
    group_dims_month = ['year', 'month', 'estado']
    if has_tipo:
        group_dims_year.append('tipo')
        group_dims_month.append('tipo')

    kpis_by_year = []
    # Grouping by dimensions
    yearly_stats = df.groupby(group_dims_year).agg({
        'facturado': 'sum',
        'pagado': 'sum',
        'pendiente': 'sum',
        'descuento': 'sum',
        'fecha_emision': 'count'
    }).reset_index()

    for _, r in yearly_stats.iterrows():
        tx_count = r['fecha_emision']
        facturado = r['facturado']
        descuento = r['descuento']
        gross = facturado + descuento
        
        item = {
            "year": int(r['year']),
            "estado": str(r['estado']),
            "tx_count": int(tx_count),
            "facturado": float(facturado),
            "pagado": float(r['pagado']),
            "pendiente": float(r['pendiente']),
            "descuento": float(descuento),
            "ticket_prom": float(facturado / tx_count) if tx_count > 0 else 0,
            "desc_rate_percentage": float((descuento / gross) * 100) if gross > 0 else 0,
        }
        if has_tipo:
            item["tipo"] = str(r['tipo'])
        kpis_by_year.append(item)

    # --- 2. YTD Comparison (2024 vs 2025 - Still using valid data for direct benchmark) ---
    df_valid = df[df['estado'] != 'ANULADO'].copy()
    max_date_2025 = df_valid[df_valid['year'] == 2025]['fecha_emision'].max()
    ytd_comparison = {}
    
    if pd.notnull(max_date_2025):
        day_of_year_limit = max_date_2025.dayofyear
        df_2025_ytd = df_valid[(df_valid['year'] == 2025) & (df_valid['fecha_emision'].dt.dayofyear <= day_of_year_limit)]
        df_2024_ytd = df_valid[(df_valid['year'] == 2024) & (df_valid['fecha_emision'].dt.dayofyear <= day_of_year_limit)]
        facturado_2025 = df_2025_ytd['facturado'].sum()
        facturado_2024 = df_2024_ytd['facturado'].sum()
        growth_rate = ((facturado_2025 - facturado_2024) / facturado_2024) * 100 if facturado_2024 > 0 else 0
        
        ytd_comparison = {
            "limit_date": max_date_2025.isoformat(),
            "facturado_2025": float(facturado_2025),
            "facturado_2024": float(facturado_2024),
            "growth_rate_percentage": float(growth_rate),
            "tx_2025": int(len(df_2025_ytd)),
            "tx_2024": int(len(df_2024_ytd))
        }

    # --- 3. Granular Monthly KPIs (with status/tipo breakdown) ---
    monthly_stats = df.groupby(group_dims_month).agg({
        'facturado': 'sum',
        'pagado': 'sum',
        'pendiente': 'sum',
        'descuento': 'sum',
        'fecha_emision': 'count'
    }).reset_index()

    kpis_by_month_list = []
    for _, r in monthly_stats.iterrows():
        tx_count = r['fecha_emision']
        item = {
            "year": int(r['year']),
            "month": int(r['month']),
            "estado": str(r['estado']),
            "facturado": float(r['facturado']),
            "pagado": float(r['pagado']),
            "pendiente": float(r['pendiente']),
            "descuento": float(r['descuento']),
            "tx_count": int(tx_count),
            "ticket_prom": float(r['facturado'] / tx_count) if tx_count > 0 else 0
        }
        if has_tipo:
            item["tipo"] = str(r['tipo'])
        kpis_by_month_list.append(item)

    # --- 4. Day of Week & Hours (Operational Analysis) ---
    df_valid['dow_idx'] = df_valid['fecha_emision'].dt.dayofweek
    dow_map = {
        0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
    }
    
    # 4a. DoW Stats for all available data in df_valid
    # Using named aggregation to avoid MultiIndex/duplicate name issues
    dow_stats_all = df_valid.groupby('dow_idx').agg(
        total_facturado=('facturado', 'sum'),
        total_tx=('fecha_emision', 'count'),
        count_active_days=('fecha_emision', lambda x: x.dt.date.nunique())
    ).reset_index()
    
    dow_data_all = []
    for _, row in dow_stats_all.iterrows():
        dow_data_all.append({
            "day": dow_map.get(int(row['dow_idx']), "Unknown"),
            "facturado": float(row['total_facturado']),
            "tx_count": int(row['total_tx']),
            "avg_daily_sales": float(row['total_facturado'] / row['count_active_days']) if row['count_active_days'] > 0 else 0
        })

    # 4b. Heatmap (Day x Hour)
    heatmap_stats = df_valid.groupby(['dow_idx', 'hour']).agg(
        total_facturado=('facturado', 'sum'),
        total_tx=('fecha_emision', 'count')
    ).reset_index()
    
    demanda_heatmap = []
    for _, row in heatmap_stats.iterrows():
        demanda_heatmap.append({
            "day": dow_map.get(int(row['dow_idx'])),
            "hour": int(row['hour']),
            "facturado": float(row['total_facturado']),
            "tx_count": int(row['total_tx'])
        })

    # 4c. Daily Trends (for Rolling Average)
    # Ensure date grouping name doesn't collide with existing columns
    daily_stats = df_valid.groupby(df_valid['fecha_emision'].dt.date.rename('date_key')).agg(
        total_facturado=('facturado', 'sum'),
        total_tx=('fecha_emision', 'count')
    ).reset_index()
    
    daily_trends = []
    for _, row in daily_stats.iterrows():
        daily_trends.append({
            "date": str(row['date_key']),
            "facturado": float(row['total_facturado']),
            "tx_count": int(row['total_tx'])
        })

    # --- 5. Payment Methods Mix ---
    # Assuming 'forma_pago_raw' needs simple categorization
    def categorize_payment(pago):
        p = str(pago).lower()
        if 'tarjeta' in p or 'transbank' in p or 'tbk' in p: return 'Tarjeta/POS'
        if 'transferencia' in p: return 'Transferencia'
        if 'efectivo' in p: return 'Efectivo'
        if 'sin boleta' in p: return 'Sin Boleta'
        return 'Otros'

    if 'forma_pago_raw' in df_valid.columns:
        df_valid['payment_type'] = df_valid['forma_pago_raw'].apply(categorize_payment)
    else:
        df_valid['payment_type'] = 'Otros'
    
    # --- 5. Payment Methods Mix & Trends ---
    # Categorize and aggregate by Year, Month and Type
    payment_stats = df_valid.groupby(['year', 'month', 'payment_type']).agg(
        amount=('facturado', 'sum'),
        count=('fecha_emision', 'count')
    ).reset_index()

    payment_mix_data = []
    for _, row in payment_stats.iterrows():
        payment_mix_data.append({
            "year": int(row['year']),
            "month": int(row['month']),
            "type": str(row['payment_type']),
            "amount": float(row['amount']),
            "count": int(row['count'])
        })

    # Legacy payment_mix for backward compatibility (Yearly percentage)
    payment_mix_legacy = []
    years = sorted(df_valid['year'].unique().tolist())
    for y in years:
        df_y = df_valid[df_valid['year'] == y]
        total_y = df_y['facturado'].sum()
        if total_y == 0: continue
        mix_y = df_y.groupby('payment_type')['facturado'].sum().reset_index()
        mix_dict = {row['payment_type']: float((row['facturado'] / total_y) * 100) for _, row in mix_y.iterrows()}
        mix_dict['year'] = int(y)
        payment_mix_legacy.append(mix_dict)

    # --- 6. Top Debtors (2025) ---
    df_2025 = df_valid[df_valid['year'] == 2025].copy()
    top_debtors_list = []
    if not df_2025.empty and 'cliente' in df_2025.columns:
        df_2025_pending = df_2025[df_2025['pendiente'] > 0]
        if not df_2025_pending.empty:
            top_debtors = df_2025_pending.groupby('cliente')['pendiente'].sum().reset_index().sort_values('pendiente', ascending=False).head(5)
            top_debtors_list = [{"cliente": str(r['cliente']), "pendiente": float(r['pendiente'])} for _, r in top_debtors.iterrows()]

    # --- 7. Customer Retention & Concentration (2025) ---
    customer_insights = {
        "total_clients_2025": 0,
        "retention_rate_percentage": 0,
        "pareto_top_20_share_percentage": 0,
        "top_20_clients": []
    }
    
    if not df_2025.empty and 'cliente' in df_2025.columns:
        customer_stats = df_2025.groupby('cliente').agg({
            'facturado': 'sum', 
            'fecha_emision': 'count'
        }).reset_index()
        
        total_clients = len(customer_stats)
        if total_clients > 0:
            returning_clients = len(customer_stats[customer_stats['fecha_emision'] > 1])
            retention_rate = (returning_clients / total_clients) * 100
            
            customer_stats = customer_stats.sort_values('facturado', ascending=False)
            top_20_rev = customer_stats.head(20)['facturado'].sum()
            total_rev = df_2025['facturado'].sum()
            pareto_share = (top_20_rev / total_rev) * 100 if total_rev > 0 else 0
            
            top_20_list = [{
                "cliente": str(r['cliente']), 
                "facturado": float(r['facturado']),
                "tx_count": int(r['fecha_emision'])
            } for _, r in customer_stats.head(20).iterrows()]
            
            customer_insights = {
                "total_clients_2025": int(total_clients),
                "retention_rate_percentage": float(retention_rate),
                "pareto_top_20_share_percentage": float(pareto_share),
                "top_20_clients": top_20_list
            }

    # --- 8. Aging Analysis (Pending 2025) ---
    aging_data = []
    if not df_2025.empty:
        df_2025_pending = df_2025[df_2025['pendiente'] > 0].copy()
        if not df_2025_pending.empty:
            ref_date = df['fecha_emision'].max()
            df_2025_pending['days_since'] = (ref_date - df_2025_pending['fecha_emision']).dt.days
            
            aging_bins = {
                "0-7 días": df_2025_pending[df_2025_pending['days_since'] <= 7]['pendiente'].sum(),
                "8-30 días": df_2025_pending[(df_2025_pending['days_since'] > 7) & (df_2025_pending['days_since'] <= 30)]['pendiente'].sum(),
                "31-60 días": df_2025_pending[(df_2025_pending['days_since'] > 30) & (df_2025_pending['days_since'] <= 60)]['pendiente'].sum(),
                "60+ días": df_2025_pending[df_2025_pending['days_since'] > 60]['pendiente'].sum()
            }
            aging_data = [{"range": k, "amount": float(v)} for k, v in aging_bins.items()]

    # --- 9. Data Quality Scan ---
    total_records = len(df)
    missing_payment = 0
    if 'forma_pago_raw' in df.columns:
        missing_payment = len(df[df['forma_pago_raw'].isnull() | (df['forma_pago_raw'] == '')])
    
    missing_client = 0
    if 'cliente' in df.columns:
        missing_client = len(df[df['cliente'].isnull() | (df['cliente'] == '')])
        
    anuladas_count = len(df[df['estado'] == 'ANULADO'])
    
    quality_metrics = {
        "total_records": int(total_records),
        "missing_payment_percentage": float((missing_payment / total_records) * 100) if total_records > 0 else 0,
        "missing_client_percentage": float((missing_client / total_records) * 100) if total_records > 0 else 0,
        "anuladas_percentage": float((anuladas_count / total_records) * 100) if total_records > 0 else 0,
        "with_discounts_percentage": float((len(df[df['descuento'] > 0]) / total_records) * 100) if total_records > 0 else 0
    }

    result = {
        "kpis_by_year": kpis_by_year,
        "ytd_comparison": ytd_comparison,
        "monthly_seasonality": kpis_by_month_list,
        "dow_analysis": dow_data_all,
        "daily_trends": daily_trends,
        "demanda_heatmap": demanda_heatmap,
        "payment_mix": payment_mix_legacy,
        "payment_mix_data": payment_mix_data,
        "top_debtors_2025": top_debtors_list,
        "customer_insights": customer_insights,
        "aging_analysis_2025": aging_data,
        "data_quality": quality_metrics
    }

    # Helper to clean result for JSON (replaces NaN with None/0)
    def sanitize(obj):
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [sanitize(i) for i in obj]
        elif isinstance(obj, float):
            if np.isnan(obj) or np.isinf(obj):
                return 0.0
            return obj
        return obj

    return sanitize(result)

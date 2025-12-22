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
    dow_stats_all = df_valid.groupby('dow_idx').agg({
        'facturado': 'sum',
        'fecha_emision': ['count', lambda x: x.dt.date.nunique()]
    }).reset_index()
    dow_stats_all.columns = ['dow_idx', 'facturado', 'tx_count', 'count_active_days']
    
    dow_data_all = []
    for _, row in dow_stats_all.iterrows():
        dow_data_all.append({
            "day": dow_map.get(int(row['dow_idx']), "Unknown"),
            "facturado": float(row['facturado']),
            "tx_count": int(row['tx_count']),
            "avg_daily_sales": float(row['facturado'] / row['count_active_days']) if row['count_active_days'] > 0 else 0
        })

    # 4b. Heatmap (Day x Hour)
    heatmap_stats = df_valid.groupby(['dow_idx', 'hour']).agg({
        'facturado': 'sum',
        'fecha_emision': 'count'
    }).reset_index()
    
    demanda_heatmap = []
    for _, row in heatmap_stats.iterrows():
        demanda_heatmap.append({
            "day": dow_map.get(int(row['dow_idx'])),
            "hour": int(row['hour']),
            "facturado": float(row['facturado']),
            "tx_count": int(row['fecha_emision'])
        })

    # 4c. Daily Trends (for Rolling Average)
    daily_stats = df_valid.groupby(df_valid['fecha_emision'].dt.date).agg({
        'facturado': 'sum',
        'fecha_emision': 'count'
    }).reset_index()
    daily_stats = daily_stats.rename(columns={'fecha_emision': 'tx_count', 'fecha_emision_dt': 'date'})
    daily_stats.columns = ['date', 'facturado', 'tx_count']
    daily_stats['date'] = daily_stats['date'].astype(str)
    
    daily_trends = daily_stats.to_dict(orient='records')

    # --- 5. Payment Methods Mix ---
    # Assuming 'forma_pago_raw' needs simple categorization
    def categorize_payment(pago):
        p = str(pago).lower()
        if 'tarjeta' in p or 'transbank' in p or 'tbk' in p: return 'Tarjeta/POS'
        if 'transferencia' in p: return 'Transferencia'
        if 'efectivo' in p: return 'Efectivo'
        if 'sin boleta' in p: return 'Sin Boleta'
        return 'Otros'

    df_valid['payment_type'] = df_valid['forma_pago_raw'].apply(categorize_payment)
    
    payment_mix = []
    years = sorted(df_valid['year'].unique().tolist())
    for y in years:
        df_y = df_valid[df_valid['year'] == y]
        total_y = df_y['facturado'].sum()
        if total_y == 0: continue
        
        mix_y = df_y.groupby('payment_type')['facturado'].sum().reset_index()
        mix_y['percentage'] = (mix_y['facturado'] / total_y) * 100
        
        mix_dict = {row['payment_type']: float(row['percentage']) for _, row in mix_y.iterrows()}
        mix_dict['year'] = int(y)
        payment_mix.append(mix_dict)

    # --- 6. Top Debtors (2025) ---
    df_2025 = df_valid[df_valid['year'] == 2025].copy()
    df_2025_pending = df_2025[df_2025['pendiente'] > 0].copy()
    top_debtors = df_2025_pending.groupby('cliente')['pendiente'].sum().reset_index().sort_values('pendiente', ascending=False).head(5)
    top_debtors_list = [{"cliente": str(r['cliente']), "pendiente": float(r['pendiente'])} for _, r in top_debtors.iterrows()]

    # --- 7. Customer Retention & Concentration (2025) ---
    customer_stats = df_2025.groupby('cliente').agg({
        'facturado': 'sum', 
        'fecha_emision': 'count'
    }).reset_index()
    
    total_clients_2025 = len(customer_stats)
    returning_clients = len(customer_stats[customer_stats['fecha_emision'] > 1])
    retention_rate = (returning_clients / total_clients_2025) * 100 if total_clients_2025 > 0 else 0
    
    # Pareto (Concentration)
    customer_stats = customer_stats.sort_values('facturado', ascending=False)
    top_20_clients_revenue = customer_stats.head(20)['facturado'].sum()
    total_revenue_2025 = df_2025['facturado'].sum()
    pareto_top_20_share = (top_20_clients_revenue / total_revenue_2025) * 100 if total_revenue_2025 > 0 else 0
    
    # Extract Top 20 clients details
    top_20_clients_list = [{
        "cliente": str(r['cliente']), 
        "facturado": float(r['facturado']),
        "tx_count": int(r['fecha_emision'])
    } for _, r in customer_stats.head(20).iterrows()]

    # --- 8. Aging Analysis (Pending 2025) ---
    # Using 'today' as user reference or max numeric date
    ref_date = df['fecha_emision'].max()
    
    # Calculate days since emission for pending
    df_2025_pending['days_since'] = (ref_date - df_2025_pending['fecha_emision']).dt.days
    
    aging_bins = {
        "0-7 días": df_2025_pending[df_2025_pending['days_since'] <= 7]['pendiente'].sum(),
        "8-30 días": df_2025_pending[(df_2025_pending['days_since'] > 7) & (df_2025_pending['days_since'] <= 30)]['pendiente'].sum(),
        "31-60 días": df_2025_pending[(df_2025_pending['days_since'] > 30) & (df_2025_pending['days_since'] <= 60)]['pendiente'].sum(),
        "60+ días": df_2025_pending[df_2025_pending['days_since'] > 60]['pendiente'].sum()
    }
    # Convert to float
    aging_data = [{"range": k, "amount": float(v)} for k, v in aging_bins.items()]

    # --- 9. Data Quality Scan ---
    total_records = len(df)
    missing_payment = len(df[df['forma_pago_raw'].isnull() | (df['forma_pago_raw'] == '')])
    missing_client = len(df[df['cliente'].isnull() | (df['cliente'] == '')])
    anuladas_count = len(df[df['estado'] == 'ANULADO'])
    
    quality_metrics = {
        "total_records": int(total_records),
        "missing_payment_percentage": float((missing_payment / total_records) * 100) if total_records > 0 else 0,
        "missing_client_percentage": float((missing_client / total_records) * 100) if total_records > 0 else 0,
        "anuladas_percentage": float((anuladas_count / total_records) * 100) if total_records > 0 else 0,
        "with_discounts_percentage": float((len(df[df['descuento'] > 0]) / total_records) * 100) if total_records > 0 else 0
    }

    return {
        "kpis_by_year": kpis_by_year,
        "ytd_comparison": ytd_comparison,
        "monthly_seasonality": kpis_by_month_list,
        "dow_analysis": dow_data_all,
        "daily_trends": daily_trends,
        "demanda_heatmap": demanda_heatmap,
        "payment_mix": payment_mix,
        "top_debtors_2025": top_debtors_list,
        "customer_insights": {
            "total_clients_2025": int(total_clients_2025),
            "retention_rate_percentage": float(retention_rate),
            "pareto_top_20_share_percentage": float(pareto_top_20_share),
            "top_20_clients": top_20_clients_list
        },
        "aging_analysis_2025": aging_data,
        "data_quality": quality_metrics
    }

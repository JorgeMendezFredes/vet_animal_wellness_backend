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
    numeric_cols = ['facturado', 'pagado', 'pendiente', 'descuento']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Convert date
    df['fecha_emision'] = pd.to_datetime(df['fecha_emision'], errors='coerce')
    df['year'] = df['fecha_emision'].dt.year
    df['month'] = df['fecha_emision'].dt.month
    df['day_name'] = df['fecha_emision'].dt.day_name() # English names by default, can map later
    df['hour'] = df['fecha_emision'].dt.hour
    
    # Filter out invalid dates if any
    df = df.dropna(subset=['fecha_emision'])

    # --- 1. KPIs per Year (Excluding ANULADO) ---
    df_valid = df[df['estado'] != 'ANULADO'].copy()
    
    kpis_by_year = []
    years = sorted(df['year'].unique())
    
    for y in years:
        df_y = df_valid[df_valid['year'] == y]
        df_y_all = df[df['year'] == y] # For cancellation rate
        
        tx_count = len(df_y)
        facturado = df_y['facturado'].sum()
        pagado = df_y['pagado'].sum()
        pendiente = df_y['pendiente'].sum()
        descuento = df_y['descuento'].sum()
        
        # Calculate rates
        gross = facturado + descuento
        ticket_prom = facturado / tx_count if tx_count > 0 else 0
        desc_rate = (descuento / gross) * 100 if gross > 0 else 0
        
        anul_count = len(df_y_all[df_y_all['estado'] == 'ANULADO'])
        total_count = len(df_y_all)
        anul_rate = (anul_count / total_count) * 100 if total_count > 0 else 0
        
        kpis_by_year.append({
            "year": int(y),
            "tx_count": int(tx_count),
            "facturado": float(facturado),
            "pagado": float(pagado),
            "pendiente": float(pendiente),
            "ticket_prom": float(ticket_prom),
            "desc_rate_percentage": float(desc_rate),
            "anul_rate_percentage": float(anul_rate)
        })

    # --- 2. YTD Comparison (2024 vs 2025) ---
    # Define "today" as the max date in dataset for fair comparison, or hardcode if requested.
    # User said "21-dic". Let's use relative YTD based on the latest date in 2025.
    
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

    # --- 3. Seasonality (Monthly) ---
    monthly_stats = df_valid.groupby(['year', 'month'])['facturado'].sum().reset_index()
    monthly_stats = monthly_stats.to_dict(orient='records')
    # Convert int64 to int/float for JSON
    monthly_stats = [{"year": int(r['year']), "month": int(r['month']), "facturado": float(r['facturado'])} for r in monthly_stats]

    # --- 4. Day of Week & Hours (2025 Analysis) ---
    df_2025 = df_valid[df_valid['year'] == 2025].copy()
    
    # DoW
    dow_map = {
        0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
    }
    df_2025['dow_idx'] = df_2025['fecha_emision'].dt.dayofweek
    dow_stats = df_2025.groupby('dow_idx').agg({
        'facturado': 'sum',
        'fecha_emision': lambda x: x.dt.date.nunique()
    }).reset_index()
    
    # Rename and force type conversion
    dow_stats = dow_stats.rename(columns={'fecha_emision': 'count_active_days'})
    dow_stats['count_active_days'] = pd.to_numeric(dow_stats['count_active_days'])
    
    dow_stats['promedio_dia'] = dow_stats['facturado'] / dow_stats['count_active_days']
    
    dow_data = []
    for _, row in dow_stats.iterrows():
        dow_data.append({
            "day": dow_map.get(int(row['dow_idx']), "Unknown"),
            "total_sales": float(row['facturado']),
            "active_days": int(row['count_active_days']),
            "avg_daily_sales": float(row['promedio_dia'])
        })

    # Hours
    hourly_stats = df_2025.groupby('hour')['facturado'].sum().reset_index().sort_values('facturado', ascending=False).head(5)
    top_hours = [{"hour": int(r['hour']), "facturado": float(r['facturado'])} for _, r in hourly_stats.iterrows()]

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
    df_2025_pending = df_2025[df_2025['pendiente'] > 0]
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
        "monthly_seasonality": monthly_stats,
        "dow_analysis_2025": dow_data,
        "top_hours_2025": top_hours,
        "payment_mix": payment_mix,
        "top_debtors_2025": top_debtors_list,
        "customer_insights": {
            "total_clients_2025": int(total_clients_2025),
            "retention_rate_percentage": float(retention_rate),
            "pareto_top_20_share_percentage": float(pareto_top_20_share)
        },
        "aging_analysis_2025": aging_data,
        "data_quality": quality_metrics
    }

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

    return {
        "kpis_by_year": kpis_by_year,
        "ytd_comparison": ytd_comparison,
        "monthly_seasonality": monthly_stats,
        "dow_analysis_2025": dow_data,
        "top_hours_2025": top_hours,
        "payment_mix": payment_mix,
        "top_debtors_2025": top_debtors_list
    }

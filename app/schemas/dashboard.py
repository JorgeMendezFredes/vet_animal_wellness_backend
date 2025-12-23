from pydantic import BaseModel
from typing import List, Dict, Optional, Any

# --- Base Building Blocks ---

class KpiItem(BaseModel):
    year: int
    month: Optional[int] = None
    estado: Optional[str] = None
    tipo: Optional[str] = None
    facturado: float
    pagado: float
    pendiente: float
    descuento: float
    tx_count: int
    count_with_discount: Optional[int] = 0

class SummaryKPIs(BaseModel):
    facturado: float
    pagado: float
    pendiente: float
    descuento: float
    tx_count: int
    avg_ticket: float
    anuladas_count: int
    anuladas_pct: float
    discount_percent: float

class DashboardSummaryResponse(BaseModel):
    summary: SummaryKPIs
    kpis_by_year: List[KpiItem]
    monthly_seasonality: List[KpiItem]
    daily_trends: List[Dict[str, Any]]
    dow_analysis: List[Dict[str, Any]]
    demanda_heatmap: List[Dict[str, Any]]

# --- Insights ---

class CustomerInsightItem(BaseModel):
    cliente: str
    facturado: float
    tx_count: int

class CustomerInsights(BaseModel):
    total_clients: int
    retention_rate: float
    pareto_share: float
    top_20_clients: List[CustomerInsightItem]

class DataQuality(BaseModel):
    total_records: int
    missing_payment_pct: float
    missing_client_pct: float
    anuladas_pct: float

class PaymentMixItem(BaseModel):
    year: int
    month: int
    type: str
    amount: float
    count: int

class DashboardInsightsResponse(BaseModel):
    customer_insights: CustomerInsights
    data_quality: DataQuality
    payment_mix_data: List[PaymentMixItem]
    aging_analysis: List[Dict[str, Any]]
    discounts_analysis: Dict[str, Any]
    anuladas_audit: List[Dict[str, Any]]

# --- Transactions ---

class TransactionItem(BaseModel):
    fecha_emision: str
    comprobante: str
    cliente: str
    facturado: float
    pagado: float
    pendiente: float
    descuento: float
    estado: str
    payment_type: str

class PendingInvoiceItem(BaseModel):
    fecha_emision: str
    comprobante: str
    cliente: str
    facturado: float
    pendiente: float
    days_overdue: int

class DashboardTransactionsResponse(BaseModel):
    drilldown_data: List[TransactionItem]
    pending_invoices: List[PendingInvoiceItem]

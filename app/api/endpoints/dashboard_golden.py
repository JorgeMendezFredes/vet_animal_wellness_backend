from fastapi import APIRouter, HTTPException
from app.db.supabase import get_supabase
from decimal import Decimal
import datetime

router = APIRouter()

def normalize_decimal(val):
    if val is None:
        return Decimal('0.00')
    return Decimal(str(val))

@router.get("/golden-verification")
async def verify_golden_dataset():
    try:
        supabase = get_supabase()
        
        # Fetch all active records logic (identical to verification script)
        all_rows = []
        start = 0
        batch_size = 1000
        
        while True:
            response = supabase.table("comprobantes")\
                .select("*")\
                .eq("is_active", True)\
                .order("id")\
                .range(start, start + batch_size - 1)\
                .execute()
            
            data = response.data
            if not data:
                break
                
            all_rows.extend(data)
            
            if len(data) < batch_size:
                break
                
            start += batch_size

        # Pre-processing: Filter out ANULADO
        valid_rows = [
            row for row in all_rows 
            if row.get('estado', '').strip().upper() != 'ANULADO'
        ]

        results = {}

        # TEST 1: Total Transactions
        expected_count = 6296
        actual_count = len(valid_rows)
        results['test_1'] = {
            "name": "Total de transacciones",
            "status": "PASS" if actual_count == expected_count else "FAIL",
            "expected": expected_count,
            "actual": actual_count
        }

        # TEST 2-5: Historical Totals
        total_facturado = sum(normalize_decimal(r['facturado']) for r in valid_rows)
        total_pagado = sum(normalize_decimal(r['pagado']) for r in valid_rows)
        total_pendiente = sum(normalize_decimal(r['pendiente']) for r in valid_rows)
        invariant = total_facturado - (total_pagado + total_pendiente)

        expected_facturado = Decimal('246404811.64')
        expected_pagado = Decimal('246085710.64')
        expected_pendiente = Decimal('319100.90')
        expected_invariant = Decimal('0.10')

        results['test_2'] = {
            "name": "Total facturado histórico",
            "status": "PASS" if total_facturado == expected_facturado else "FAIL",
            "expected": float(expected_facturado),
            "actual": float(total_facturado)
        }
        results['test_3'] = {
            "name": "Total pagado histórico",
            "status": "PASS" if total_pagado == expected_pagado else "FAIL",
            "expected": float(expected_pagado),
            "actual": float(total_pagado)
        }
        results['test_4'] = {
            "name": "Total pendiente histórico",
            "status": "PASS" if total_pendiente == expected_pendiente else "FAIL",
            "expected": float(expected_pendiente),
            "actual": float(total_pendiente)
        }
        results['test_5'] = {
            "name": "Invariante contable global",
            "status": "PASS" if invariant == expected_invariant else "FAIL",
            "expected": float(expected_invariant),
            "actual": float(invariant)
        }

        # TEST 6: Annual Totals
        annual_stats = {}
        for row in valid_rows:
            year = row.get('source_year')
            if not year:
                fe = row.get('fecha_emision')
                if fe:
                     year = int(fe[:4])
            
            if year:
                if year not in annual_stats:
                    annual_stats[year] = {
                        'count': 0,
                        'facturado': Decimal('0.00'),
                        'pagado': Decimal('0.00'),
                        'pendiente': Decimal('0.00')
                    }
                annual_stats[year]['count'] += 1
                annual_stats[year]['facturado'] += normalize_decimal(row['facturado'])
                annual_stats[year]['pagado'] += normalize_decimal(row['pagado'])
                annual_stats[year]['pendiente'] += normalize_decimal(row['pendiente'])

        expected_annual = {
            2022: {'count': 49, 'facturado': Decimal('1923180.00'), 'pagado': Decimal('1923180.00'), 'pendiente': Decimal('0.00')},
            2023: {'count': 1302, 'facturado': Decimal('55458499.90'), 'pagado': Decimal('55458499.90'), 'pendiente': Decimal('0.00')},
            2024: {'count': 2350, 'facturado': Decimal('89765761.64'), 'pagado': Decimal('89765761.64'), 'pendiente': Decimal('0.00')},
            2025: {'count': 2595, 'facturado': Decimal('99257370.10'), 'pagado': Decimal('98938269.10'), 'pendiente': Decimal('319100.90')},
        }

        annual_results = []
        all_annual_pass = True
        
        # Sort years to ensure consistent order
        sorted_years = sorted(expected_annual.keys())
        
        for year in sorted_years:
            stats = expected_annual[year]
            actual = annual_stats.get(year)
            
            year_pass = True
            if not actual:
                year_pass = False
            else:
                if actual['count'] != stats['count']: year_pass = False
                if actual['facturado'] != stats['facturado']: year_pass = False
                if actual['pagado'] != stats['pagado']: year_pass = False
                if actual['pendiente'] != stats['pendiente']: year_pass = False
            
            if not year_pass:
                all_annual_pass = False

            annual_results.append({
                "year": year,
                "status": "PASS" if year_pass else "FAIL",
                "expected": {k: float(v) if isinstance(v, Decimal) else v for k,v in stats.items()},
                "actual": {k: float(v) if isinstance(v, Decimal) else v for k,v in actual.items()} if actual else None
            })

        results['test_6'] = {
            "name": "Totales anuales",
            "status": "PASS" if all_annual_pass else "FAIL",
            "details": annual_results
        }

        # TEST 7: Operational States
        pagado_count = sum(1 for r in valid_rows if r.get('estado', '').strip().upper() == 'PAGADO')
        pendiente_count = sum(1 for r in valid_rows if r.get('estado', '').strip().upper() == 'PENDIENTE')
        
        states_pass = (pagado_count == 6285 and pendiente_count == 11 and (pagado_count + pendiente_count) == 6296)
        
        results['test_7'] = {
            "name": "Estados operacionales reales",
            "status": "PASS" if states_pass else "FAIL",
            "expected": {"PAGADO": 6285, "PENDIENTE": 11, "TOTAL": 6296},
            "actual": {"PAGADO": pagado_count, "PENDIENTE": pendiente_count, "TOTAL": pagado_count + pendiente_count}
        }

        # TEST 8: CxC Real
        cxc_rows = [r for r in valid_rows if normalize_decimal(r.get('pendiente')) > 0]
        cxc_count = len(cxc_rows)
        cxc_sum = sum(normalize_decimal(r.get('pendiente')) for r in cxc_rows)
        
        cxc_pass = (cxc_count == 11 and cxc_sum == expected_pendiente)
        
        results['test_8'] = {
            "name": "CxC real (pendientes)",
            "status": "PASS" if cxc_pass else "FAIL",
            "expected": {"count": 11, "sum": float(expected_pendiente)},
            "actual": {"count": cxc_count, "sum": float(cxc_sum)}
        }

        # TEST 9: Anchor Record
        target_comprobante = "BOLETA: 001 - 004865"
        anchor_record = next((r for r in valid_rows if r.get('comprobante') == target_comprobante), None)
        
        anchor_pass = False
        anchor_details = {}
        
        if anchor_record:
            ar_facturado = normalize_decimal(anchor_record['facturado'])
            ar_pagado = normalize_decimal(anchor_record['pagado'])
            ar_pendiente = normalize_decimal(anchor_record['pendiente'])
            ar_delta = ar_facturado - (ar_pagado + ar_pendiente)
            
            anchor_pass = (
                ar_facturado == Decimal('623400.10') and
                ar_pagado == Decimal('623400.00') and
                ar_pendiente == Decimal('0.00') and
                ar_delta == Decimal('0.10')
            )
            anchor_details = {
                 "facturado": float(ar_facturado),
                 "pagado": float(ar_pagado),
                 "pendiente": float(ar_pendiente),
                 "delta": float(ar_delta)
            }
        
        results['test_9'] = {
            "name": "Registro ancla (Delta 0.10)",
            "status": "PASS" if anchor_pass else "FAIL",
            "expected": {"facturado": 623400.10, "pagado": 623400.00, "delta": 0.10},
            "actual": anchor_details
        }

        return results

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

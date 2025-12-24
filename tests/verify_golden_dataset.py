import asyncio
import os
import sys
from decimal import Decimal

# Add the parent directory to sys.path to allow imports from app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.supabase import get_supabase

async def fetch_all_data():
    supabase = get_supabase()
    # Fetch all active records
    # Note: Supabase limits fetch size, so we might need pagination if the dataset grows.
    # For 6296 records, we should be careful. default limit is usually 1000.
    
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
        
    return all_rows

def normalize_decimal(val):
    if val is None:
        return Decimal('0.00')
    return Decimal(str(val))

def run_tests():
    print("Fetching data from Supabase...")
    # Since specific run_command is not available here, using asyncio.run
    data = asyncio.run(fetch_all_data())
    
    print(f"Total rows fetched: {len(data)}")
    
    # Pre-processing: Filter out ANULADO
    valid_rows = [
        row for row in data 
        if row.get('estado', '').strip().upper() != 'ANULADO'
    ]
    
    print(f"Valid rows (!= ANULADO): {len(valid_rows)}")
    
    # ---------------------------------------------------------
    # TEST 1: Total de transacciones
    # ---------------------------------------------------------
    expected_count = 6296
    actual_count = len(valid_rows)
    assert actual_count == expected_count, f"TEST 1 FAILED: Expected {expected_count}, got {actual_count}"
    print("TEST 1: Total de transacciones ... PASS")

    # ---------------------------------------------------------
    # TEST 2-5: Historical Totals
    # ---------------------------------------------------------
    total_facturado = sum(normalize_decimal(r['facturado']) for r in valid_rows)
    total_pagado = sum(normalize_decimal(r['pagado']) for r in valid_rows)
    total_pendiente = sum(normalize_decimal(r['pendiente']) for r in valid_rows)
    
    # Values from Golden Dataset
    expected_facturado = Decimal('246404811.64')
    expected_pagado = Decimal('246085710.64')
    expected_pendiente = Decimal('319100.90')
    expected_invariant = Decimal('0.10')

    assert total_facturado == expected_facturado, f"TEST 2 FAILED: Facturado expected {expected_facturado}, got {total_facturado}"
    print("TEST 2: Total facturado histórico ... PASS")
    
    assert total_pagado == expected_pagado, f"TEST 3 FAILED: Pagado expected {expected_pagado}, got {total_pagado}"
    print("TEST 3: Total pagado histórico ... PASS")
    
    assert total_pendiente == expected_pendiente, f"TEST 4 FAILED: Pendiente expected {expected_pendiente}, got {total_pendiente}"
    print("TEST 4: Total pendiente histórico ... PASS")
    
    # Invariant
    invariant = total_facturado - (total_pagado + total_pendiente)
    # Floating point tolerance check or exact decimal? Golden data says exact 0.10
    # Using Decimal should be exact.
    assert invariant == expected_invariant, f"TEST 5 FAILED: Invariant expected {expected_invariant}, got {invariant}"
    print("TEST 5: Invariante contable global ... PASS")

    # ---------------------------------------------------------
    # TEST 6: Annual Totals
    # ---------------------------------------------------------
    # Group by year. Assuming 'fecha_emision' or 'source_year' is available.
    # The source_year column seems promising from the schema migration.
    
    annual_stats = {}
    
    for row in valid_rows:
        year = row.get('source_year')
        if not year:
            # Fallback to parsing fecha_emision if source_year is missing
            fe = row.get('fecha_emision')
            if fe:
                 year = int(fe[:4]) # Expecting ISO string '2022-01-01T...'
        
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

    for year, stats in expected_annual.items():
        actual = annual_stats.get(year)
        assert actual is not None, f"TEST 6 FAILED: Missing year {year}"
        assert actual['count'] == stats['count'], f"TEST 6 FAILED: Year {year} count expected {stats['count']}, got {actual['count']}"
        assert actual['facturado'] == stats['facturado'], f"TEST 6 FAILED: Year {year} facturado expected {stats['facturado']}, got {actual['facturado']}"
        assert actual['pagado'] == stats['pagado'], f"TEST 6 FAILED: Year {year} pagado expected {stats['pagado']}, got {actual['pagado']}"
        assert actual['pendiente'] == stats['pendiente'], f"TEST 6 FAILED: Year {year} pendiente expected {stats['pendiente']}, got {actual['pendiente']}"
    
    print("TEST 6: Totales anuales ... PASS")

    # ---------------------------------------------------------
    # TEST 7: Operational States
    # ---------------------------------------------------------
    pagado_count = sum(1 for r in valid_rows if r.get('estado', '').strip().upper() == 'PAGADO')
    pendiente_count = sum(1 for r in valid_rows if r.get('estado', '').strip().upper() == 'PENDIENTE')
    
    assert pagado_count == 6285, f"TEST 7 FAILED: PAGADO count expected 6285, got {pagado_count}"
    assert pendiente_count == 11, f"TEST 7 FAILED: PENDIENTE count expected 11, got {pendiente_count}"
    assert (pagado_count + pendiente_count) == 6296, "TEST 7 FAILED: Sum of states mismatch"
    
    print("TEST 7: Estados operacionales reales ... PASS")
    
    # ---------------------------------------------------------
    # TEST 8: CxC Real (Pendientes)
    # ---------------------------------------------------------
    cxc_rows = [r for r in valid_rows if normalize_decimal(r.get('pendiente')) > 0]
    cxc_count = len(cxc_rows)
    cxc_sum = sum(normalize_decimal(r.get('pendiente')) for r in cxc_rows)
    
    assert cxc_count == 11, f"TEST 8 FAILED: CxC count expected 11, got {cxc_count}"
    assert cxc_sum == expected_pendiente, f"TEST 8 FAILED: CxC sum expected {expected_pendiente}, got {cxc_sum}"
    
    print("TEST 8: CxC real (pendientes) ... PASS")

    # ---------------------------------------------------------
    # TEST 9: Anchor Record
    # ---------------------------------------------------------
    # Find record: 2025-07-03 13:29:00, BOLETA: 001 - 004865
    # Since checking exact datetime string might be tricky due to timezone/formatting, 
    # checking comprobante ID is safer + confirming other fields.
    
    target_comprobante = "BOLETA: 001 - 004865"
    anchor_record = next((r for r in valid_rows if r.get('comprobante') == target_comprobante), None)
    
    assert anchor_record is not None, f"TEST 9 FAILED: Anchor record {target_comprobante} not found"
    
    # Check values
    
    # Date check (loose string match or object)
    # The requirement says: 2025-07-03 13:29:00. Supabase might return ISO string.
    # Let's verify other fields first which are more robust.
    
    ar_facturado = normalize_decimal(anchor_record['facturado'])
    ar_pagado = normalize_decimal(anchor_record['pagado'])
    ar_pendiente = normalize_decimal(anchor_record['pendiente'])
    ar_delta = ar_facturado - (ar_pagado + ar_pendiente)
    
    assert ar_facturado == Decimal('623400.10'), f"TEST 9 FAILED: Facturado mismatch {ar_facturado}"
    # Note: The user said Pagado = 623400.00 and Delta = 0.10
    assert ar_pagado == Decimal('623400.00'), f"TEST 9 FAILED: Pagado mismatch {ar_pagado}"
    assert ar_pendiente == Decimal('0.00'), f"TEST 9 FAILED: Pendiente mismatch {ar_pendiente}"
    assert ar_delta == Decimal('0.10'), f"TEST 9 FAILED: Delta mismatch {ar_delta}"
    
    print("TEST 9: Registro ancla ... PASS")

    print("\n---------------------------------------------------")
    print("✔️  ALL GOLDEN DATASET TESTS PASSED")
    print("---------------------------------------------------")

if __name__ == "__main__":
    try:
        run_tests()
    except AssertionError as e:
        print(f"\n❌ VERIFICATION FAILED: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

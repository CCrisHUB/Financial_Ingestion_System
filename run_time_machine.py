# run_time_machine.py
import os
import builtins
import chase_etl
import json
import re

print("================================================================================")
print("INITIATING 15-MONTH TIME MACHINE STRESS TEST")
print("================================================================================")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_CORE_DIR = os.path.join(BASE_DIR, 'test_env', 'core_files')
TEST_INPUT_DIR = os.path.join(BASE_DIR, 'test_env', 'input_data')

chase_etl.CORE_DIR = TEST_CORE_DIR
chase_etl.INPUT_DIR = TEST_INPUT_DIR

# 1. Force the Ledger Limit to 5 so we can watch culling happen immediately
original_load_constants = chase_etl.load_constants
def mock_load_constants(filepath):
    raw, cats, sys = original_load_constants(filepath)
    raw = re.sub(r'CONST_MAX_LEDGER_ROWS = \d+', 'CONST_MAX_LEDGER_ROWS = 5', raw)
    return raw, cats, sys
chase_etl.load_constants = mock_load_constants

# 2. Auto-Responder: Map all unknown merchants to 'X' (Anomaly)
original_input = builtins.input
def mock_input(prompt):
    if "execute mccoy" in prompt.lower(): return "N"
    if "Enter Code" in prompt: return "X"
    return original_input(prompt)
builtins.input = mock_input

# 3. Generate 15 Months of Time
months = [f"2024-{str(m).zfill(2)}" for m in range(1, 13)] + ["2025-01", "2025-02", "2025-03"]

for i, month in enumerate(months):
    print(f"\n--- SIMULATING MONTH: {month} ---")
    
    # Create a dummy CSV for this specific month
    csv_path = os.path.join(TEST_INPUT_DIR, f"Chase_Dummy_{month}.csv")
    with open(csv_path, 'w') as f:
        f.write("Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n")
        f.write(f"{month}-05,{month}-05,Spectrum,Bills,Sale,-69.99,\n") # Immortal Static Bill
        f.write(f"{month}-10,{month}-10,WAL-MART,Groceries,Sale,-100.00,\n") # Immortal Variable Spend
        f.write(f"{month}-15,{month}-15,Random Roadside Motel {i},Travel,Sale,-50.00,\n") # Mortal Anomaly
        
    # Run the ETL
    try:
        chase_etl.main()
    except SystemExit:
        pass
        
    # Delete the CSV so the next loop moves forward in time
    os.remove(csv_path)
    
    # Read the resulting Ledger to prove state persistence
    ledger_file = chase_etl.get_latest_file(TEST_CORE_DIR, 'Financial_Mapping_Ledger', '.txt')
    with open(ledger_file, 'r') as f:
        content = f.read()
        
        # Check Accumulator Window
        acc_match = re.search(r'\[ACCUMULATOR_LEDGER_JSONS\]\n-+\n(.*?)(?:\n-+|$)', content, re.DOTALL)
        if acc_match:
            accs = json.loads(acc_match.group(1).strip())
            food_acc = accs.get('Food', {})
            window_size = len([k for k in food_acc.keys() if k != '_Historical_Average'])
            oldest_month = min([k for k in food_acc.keys() if k != '_Historical_Average']) if window_size > 0 else "N/A"
            print(f"  > Food Accumulator Window: {window_size} months (Oldest: {oldest_month})")
            
        # Check Ledger Culling
        rows = [line for line in content.splitlines() if '|' in line and 'Target_Label' not in line]
        print(f"  > Total Ledger Rows: {len(rows)} / 5 Max")
        
        # Verify Immortal vs Mortal
        has_walmart = any("WAL-MART" in r for r in rows)
        print(f"  > Is WAL-MART (Core) still in memory? {has_walmart}")

print("\n================================================================================")
print("TIME MACHINE TEST COMPLETE.")
print("================================================================================")
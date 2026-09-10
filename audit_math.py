# audit_math.py
import os
import re
import json

CORE_DIR = 'core_files'

def get_latest_file(directory, prefix, extension):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(extension)]
    if not files: return None
    def extract_version(filename):
        match = re.search(r'_v(\d+)\.', filename)
        return int(match.group(1)) if match else 0
    return os.path.join(directory, sorted(files, key=extract_version, reverse=True)[0])

def run_audit():
    print("================================================================================")
    print("SYSTEM ARCHITECT MATH AUDITOR")
    print("================================================================================")
    
    ledger_file = get_latest_file(CORE_DIR, 'Financial_Mapping_Ledger', '.txt')
    payload_file = get_latest_file(CORE_DIR, 'Ingestion_Expense_Payload', '.txt')
    
    if not ledger_file or not payload_file:
        print("ERROR: Could not find Ledger or Payload files.")
        return

    with open(ledger_file, 'r', encoding='utf-8') as f: ledger_raw = f.read()
    acc_match = re.search(r'\[ACCUMULATOR_LEDGER_JSONS\]\n-+\n(.*?)(?:\n-+|$)', ledger_raw, re.DOTALL)
    if not acc_match:
        print("ERROR: Could not find JSON memory in Ledger.")
        return
    accumulators = json.loads(acc_match.group(1).strip())

    with open(payload_file, 'r', encoding='utf-8') as f: payload_raw = f.read()
    
    print(f"Auditing Ledger: {os.path.basename(ledger_file)}")
    print(f"Auditing Payload: {os.path.basename(payload_file)}\n")
    
    print("--------------------------------------------------------------------------------")
    print("VARIABLE SPEND (11-Month Rolling Averages)")
    print("--------------------------------------------------------------------------------")
    for category, months_data in accumulators.items():
        payload_match_var = re.search(rf'\*\s*{category}:\s*\[Historical_Monthly_Average:\s*\$([0-9.]+)\]', payload_raw)
        if payload_match_var:
            print(f"\n--- {category.upper()} ---")
            total = 0.0
            count = 0
            for month, amount in sorted(months_data.items()):
                if month == '_Historical_Average': continue
                print(f"  {month}: ${amount:.2f}")
                total += amount
                count += 1
            if count == 0: continue
            calculated_avg = total / count
            print(f"  -> MATH: ${total:.2f} / {count} months = ${calculated_avg:.2f}")
            
            payload_avg = float(payload_match_var.group(1))
            if abs(calculated_avg - payload_avg) < 0.02:
                print(f"  -> STATUS: [PASS] Payload matches memory exactly (${payload_avg:.2f})")
            else:
                print(f"  -> STATUS: [FAIL] Payload says ${payload_avg:.2f}, but math says ${calculated_avg:.2f}")

    print("\n--------------------------------------------------------------------------------")
    print("STATIC MONTHLY BILLS (Latest Month Amount)")
    print("--------------------------------------------------------------------------------")
    for category, months_data in accumulators.items():
        payload_match_static = re.search(rf'\*\s*{category}:\s*\[Amount:\s*\$([0-9.]+)\]', payload_raw)
        if payload_match_static:
            print(f"\n--- {category.upper()} ---")
            for month, amount in sorted(months_data.items()):
                if month == '_Historical_Average': continue
                print(f"  {month}: ${amount:.2f}")
            
            latest_month = max([m for m in months_data.keys() if m != '_Historical_Average'])
            latest_amount = months_data[latest_month]
            
            payload_amt = float(payload_match_static.group(1))
            if abs(latest_amount - payload_amt) < 0.02:
                print(f"  -> STATUS: [PASS] Payload matches latest month memory (${payload_amt:.2f})")
            else:
                print(f"  -> STATUS: [FAIL] Payload says ${payload_amt:.2f}, but latest memory says ${latest_amount:.2f}")

if __name__ == "__main__":
    run_audit()
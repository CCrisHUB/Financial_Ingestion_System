#chase_etl.py
#"""
#Chase ETL Pipeline
#Date: 2026-09-08
#Version: 2.3
#Role: Ingests Chase CSVs, maps transactions, and updates accumulators.
#"""
#__version__ = "2.3"
#__date__ = "2026-09-08"

import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- DIRECTORY CONFIGURATION ---
CORE_DIR = 'core_files'
INPUT_DIR = 'input_data'

def get_latest_file(directory, prefix, extension):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(extension)]
    if not files: return None
    def extract_version(filename):
        match = re.search(r'_v(\d+)\.', filename)
        return int(match.group(1)) if match else 0
    return os.path.join(directory, sorted(files, key=extract_version, reverse=True)[0])

def get_keyword_file(directory, keyword, extension):
    files = [f for f in os.listdir(directory) if keyword.lower() in f.lower() and f.endswith(extension)]
    if not files: return None
    def extract_version(filename):
        match = re.search(r'_v(\d+)\.', filename)
        return int(match.group(1)) if match else 0
    # Fallback to mtime only if versioning isn't standard on keyword files, but prioritize version if it exists
    try:
        return os.path.join(directory, sorted(files, key=extract_version, reverse=True)[0])
    except Exception:
        return sorted([os.path.join(directory, f) for f in files], key=os.path.getmtime, reverse=True)[0]

print("Initializing Chase ETL Pipeline...")

# --- FILE DISCOVERY ---
chase_file = get_latest_file(INPUT_DIR, 'Chase', '.csv')
amazon_order_file = get_keyword_file(INPUT_DIR, 'order', '.csv')
amazon_refund_file = get_keyword_file(INPUT_DIR, 'refund', '.csv')
ledger_file = get_latest_file(CORE_DIR, 'Financial_Mapping_Ledger', '.txt')
payload_file = get_latest_file(CORE_DIR, 'Ingestion_Expense_Payload', '.txt')
constants_file = get_latest_file(CORE_DIR, 'GEM_Financial_Ingestion_Constants', '.txt')

def fatal_error(msg):
    print("\n" + "="*80)
    print(f"FATAL ERROR: {msg}")
    print("="*80 + "\n")
    raise SystemExit("HALT.")

if not chase_file: fatal_error("Missing Chase CSV in input_data folder.")
if not ledger_file: fatal_error("Missing Financial_Mapping_Ledger in core_files folder.")
if not payload_file: fatal_error("Missing Ingestion_Expense_Payload in core_files folder.")
if not constants_file: fatal_error("Missing GEM_Financial_Ingestion_Constants in core_files folder.")

# --- PARSE CONSTANTS ---
with open(constants_file, 'r', encoding='utf-8') as f: constants_raw = f.read()

def get_consts(name, text):
    m = re.search(rf'-\s*{name}.*?\n(.*?)(?=\n\s*-|\Z)', text, re.DOTALL)
    if m: return [x.strip().replace('* ', '') for x in m.group(1).split('\n') if x.strip().startswith('*')]
    return []

CONST_NON_LINEAR_EXPENSES = get_consts('CONST_NON_LINEAR_EXPENSES', constants_raw)
CONST_STATIC_MONTHLY_BILLS = get_consts('CONST_STATIC_MONTHLY_BILLS', constants_raw)
CONST_VARIABLE_SPEND = get_consts('CONST_VARIABLE_SPEND', constants_raw)
CONST_SEASONAL_MONTHLY_BILLS = get_consts('CONST_SEASONAL_MONTHLY_BILLS', constants_raw)
ALL_CATEGORIES = CONST_NON_LINEAR_EXPENSES + CONST_STATIC_MONTHLY_BILLS + CONST_VARIABLE_SPEND + CONST_SEASONAL_MONTHLY_BILLS

new_categories_added = []

# --- PARSE LEDGER ---
with open(ledger_file, 'r', encoding='utf-8') as f: ledger_raw = f.read()

ledger_rows = []
in_table = False
for line in ledger_raw.splitlines():
    if line.startswith('Merchant_String'): in_table = True; continue
    if in_table and line.startswith('---'): continue
    if in_table and line.strip() == '': in_table = False
    if in_table and '|' in line:
        parts = [p.strip() for p in line.rsplit('|', 4)]
        if len(parts) == 5: ledger_rows.append({'Merchant_String': parts[0], 'Target_Label': parts[1], 'Status': parts[2], 'Hit_Count': int(parts[3]), 'Last_Seen': parts[4]})

acc_match = re.search(r'\[ACCUMULATOR_LEDGER_JSONS\]\n-+\n(.*?)(?:\n-+|$)', ledger_raw, re.DOTALL)
accumulators = json.loads(acc_match.group(1).strip()) if acc_match else {}
buf_match = re.search(r'\[PROCESSED_MONTHS_BUFFER\]\n-+\n(.*?)(?:\n-+|$)', ledger_raw, re.DOTALL)
processed_buffer = json.loads(buf_match.group(1).strip()) if buf_match else {"Chase": [], "McCoy": []}

# --- PARSE PAYLOAD ---
historical_payload = {}
for cat in CONST_STATIC_MONTHLY_BILLS: historical_payload[cat] = "[Amount: $0.00]"
for cat in CONST_VARIABLE_SPEND: historical_payload[cat] = "[Historical_Monthly_Average: $0.00]"
for cat in CONST_NON_LINEAR_EXPENSES: historical_payload[cat] = "[Due_Month: 1] [Last_Paid_Amount: $0.00]"
for cat in CONST_SEASONAL_MONTHLY_BILLS: historical_payload[cat] = "[Pending: $0.00]"

with open(payload_file, 'r', encoding='utf-8') as f:
    for line in f:
        m = re.match(r'^\s*\*\s*([^:]+):\s*(.+)', line)
        if m: historical_payload[m.group(1).strip()] = m.group(2).strip()

# --- INGEST CHASE ---
chase_df = pd.read_csv(chase_file)
if chase_df.empty: fatal_error(f"{chase_file} is empty.")

chase_df['Date'] = pd.to_datetime(chase_df['Transaction Date'])
chase_df['Abs_Amount'] = pd.to_numeric(chase_df['Amount'], errors='coerce').abs()
latest_month = chase_df['Date'].dt.to_period('M').max().strftime('%Y-%m')

if latest_month in processed_buffer.get('Chase', []):
    fatal_error(f"Statement for {latest_month} was already processed. Duplicate detected.")

# --- INGEST & UNIFY RAW AMAZON CSVs ---
def parse_amazon_raw(filepath, is_refund=False):
    if not filepath: return pd.DataFrame()
    try: df = pd.read_csv(filepath)
    except Exception: return pd.DataFrame()
    
    # Hardcoded exact column names based on Amazon's schema
    if is_refund:
        date_col = 'Refund Date' if 'Refund Date' in df.columns else 'Creation Date'
        amt_col = 'Refund Amount' if 'Refund Amount' in df.columns else None
        prod_col = 'Reversal Reason' if 'Reversal Reason' in df.columns else None
        ship_col = None
    else:
        date_col = 'Order Date' if 'Order Date' in df.columns else None
        # Prioritize the final charged amount (which includes tax) to match Chase
        amt_col = next((c for c in ['Total Owed', 'Total Amount', 'Item Total'] if c in df.columns), None)
        if not amt_col: # Fallback
            amt_col = next((c for c in df.columns if 'total' in c.lower() or 'owed' in c.lower()), None)
        prod_col = 'Product Name' if 'Product Name' in df.columns else None
        ship_col = 'Shipment Date' if 'Shipment Date' in df.columns else None

    if not (date_col and amt_col): return pd.DataFrame()

    parsed = pd.DataFrame()
    parsed['Date'] = pd.to_datetime(df[date_col], utc=True, errors='coerce').dt.tz_localize(None)
    parsed['Total_Amount'] = df[amt_col].astype(str).str.replace(r'[$,]', '', regex=True)
    parsed['Total_Amount'] = pd.to_numeric(parsed['Total_Amount'], errors='coerce')
    
    if is_refund:
        parsed['Total_Amount'] = -parsed['Total_Amount'].abs()
        parsed['Ship_Date_Parsed'] = pd.NaT
    else:
        parsed['Total_Amount'] = parsed['Total_Amount'].abs()
        parsed['Ship_Date_Parsed'] = pd.to_datetime(df[ship_col], utc=True, errors='coerce').dt.tz_localize(None) if ship_col else pd.NaT

    parsed['Product_Name'] = df[prod_col] if prod_col else "Amazon Item"
    parsed['Join_Date'] = parsed['Ship_Date_Parsed'].fillna(parsed['Date'])
    parsed['Abs_Amount'] = parsed['Total_Amount'].abs()
    
    return parsed.dropna(subset=['Total_Amount', 'Date'])

amazon_frames = []
if amazon_order_file: amazon_frames.append(parse_amazon_raw(amazon_order_file, is_refund=False))
if amazon_refund_file: amazon_frames.append(parse_amazon_raw(amazon_refund_file, is_refund=True))

amazon_df = pd.concat(amazon_frames, ignore_index=True) if amazon_frames else pd.DataFrame()

merged_df = chase_df.copy()
merged_df['Product_Name'] = None

if not amazon_df.empty:
    for i, chase_row in merged_df.iterrows():
        if 'amazon' in str(chase_row['Description']).lower() or 'amzn' in str(chase_row['Description']).lower():
            amt = chase_row['Abs_Amount']
            c_date = chase_row['Date']
            # Match on exact amount, within 5 days (TIGHT MATCH)
            matches = amazon_df[(amazon_df['Abs_Amount'] == amt) & ((amazon_df['Join_Date'] - c_date).dt.days.abs() <= 5)]
            if not matches.empty:
                merged_df.at[i, 'Product_Name'] = matches.iloc[0]['Product_Name']
                amazon_df = amazon_df.drop(matches.index[0])

if not amazon_df.empty:
    for i, chase_row in merged_df.iterrows():
        if pd.isna(merged_df.at[i, 'Product_Name']) and ('amazon' in str(chase_row['Description']).lower() or 'amzn' in str(chase_row['Description']).lower()):
            amt = chase_row['Abs_Amount']
            c_date = chase_row['Date']
            # Match on exact amount, within 21 days (LOOSE MATCH)
            matches = amazon_df[(amazon_df['Abs_Amount'] == amt) & ((amazon_df['Join_Date'] - c_date).dt.days.abs() <= 21)]
            if not matches.empty:
                merged_df.at[i, 'Product_Name'] = matches.iloc[0]['Product_Name']
                amazon_df = amazon_df.drop(matches.index[0])

# --- MAPPING & THRESHOLD ROUTING ---
def evaluate_threshold(amount, threshold_str):
    rules = [r.strip() for r in threshold_str.split(',')]
    for rule in rules:
        if '=' not in rule: continue
        cond, label = rule.split('=', 1)
        cond, label = cond.strip(), label.strip()
        if cond.startswith('<=') and amount <= float(cond[2:]): return label
        elif cond.startswith('>=') and amount >= float(cond[2:]): return label
        elif cond.startswith('<') and amount < float(cond[1:]): return label
        elif cond.startswith('>') and amount > float(cond[1:]): return label
    return '[REQUIRES_MANUAL_REVIEW]'

merged_df['Mapped_Label'] = None
for i, row in merged_df.iterrows():
    desc = str(row['Product_Name']) if pd.notna(row['Product_Name']) else str(row['Description'])
    amt = row['Abs_Amount']
    mapped = False
    for lr in ledger_rows:
        if lr['Merchant_String'].lower() == desc.lower():
            target = lr['Target_Label']
            if '<' in target or '>' in target: target = evaluate_threshold(amt, target)
            merged_df.at[i, 'Mapped_Label'] = target
            mapped = True
            break
    if not mapped:
        for lr in ledger_rows:
            if '*' in lr['Merchant_String']:
                pattern = '^' + re.escape(lr['Merchant_String']).replace('\\*', '.*') + '$'
                if re.match(pattern, desc, re.IGNORECASE):
                    target = lr['Target_Label']
                    if '<' in target or '>' in target: target = evaluate_threshold(amt, target)
                    merged_df.at[i, 'Mapped_Label'] = target
                    mapped = True
                    break

# --- TERMINAL EXCEPTION GATE ---
exceptions = merged_df[merged_df['Mapped_Label'].isna() | (merged_df['Mapped_Label'] == '[REQUIRES_MANUAL_REVIEW]')].copy()

if not exceptions.empty:
    shorthand_map = {}
    code_idx = 0
    for cat in ALL_CATEGORIES:
        while True:
            code, temp = "", code_idx
            while temp >= 0:
                code = chr(temp % 26 + 65) + code
                temp = temp // 26 - 1
            code_idx += 1
            if code not in ['X', 'Y']:
                break
        shorthand_map[code] = cat
        
    print("\n" + "="*80)
    print(f"MANUAL REVIEW REQUIRED FOR {len(exceptions)} TRANSACTIONS")
    print("="*80)
    print("[X] IGNORE_ANOMALY_OR_DISCRETIONARY       [Y] IGNORE_TRANSFER")
    
    groups = [("NON-LINEAR", CONST_NON_LINEAR_EXPENSES), ("STATIC BILLS", CONST_STATIC_MONTHLY_BILLS), ("VARIABLE SPEND", CONST_VARIABLE_SPEND), ("SEASONAL", CONST_SEASONAL_MONTHLY_BILLS)]
    for title, cat_list in groups:
        if not cat_list: continue
        print(f"\n--- {title} ---")
        for i in range(0, len(cat_list), 2):
            c1 = cat_list[i]
            k1 = [k for k, v in shorthand_map.items() if v == c1][0]
            s1 = f"[{k1}] {c1}"
            if i + 1 < len(cat_list):
                c2 = cat_list[i+1]
                k2 = [k for k, v in shorthand_map.items() if v == c2][0]
                s2 = f"[{k2}] {c2}"
                print(f"{s1:<38} {s2}")
            else:
                print(s1)
    print("-" * 80)
    
    for idx, row in exceptions.iterrows():
        desc_prod = str(row['Product_Name']) if pd.notna(row['Product_Name']) else str(row['Description'])
        amt = row['Amount']
        sign = "-" if amt < 0 else "+"
        
        while True:
            print(f"\nDate: {row['Date'].strftime('%Y-%m-%d')} | Amount: {sign}${abs(amt):.2f}")
            print(f"Desc: {desc_prod}")
            user_input = input("Enter Code (e.g. A, AA), X/Y, or NewCat|type: ").strip().upper().rstrip(',.')
            
            if user_input == 'X': target_label = '[IGNORE_ANOMALY_OR_DISCRETIONARY]'
            elif user_input == 'Y': target_label = '[IGNORE_TRANSFER]'
            elif user_input in shorthand_map: target_label = shorthand_map[user_input]
            elif user_input in [c.upper() for c in ALL_CATEGORIES]: target_label = next(c for c in ALL_CATEGORIES if c.upper() == user_input)
            elif '|' in user_input:
                new_cat, cat_type = user_input.split('|', 1)
                new_cat, cat_type = new_cat.strip(), cat_type.strip().lower()
                # Restore original casing for new category
                original_input = input(f"Confirm exact casing for new category '{new_cat}': ").strip()
                if original_input: new_cat = original_input
                
                type_map = {'var': 'CONST_VARIABLE_SPEND', 'fix': 'CONST_STATIC_MONTHLY_BILLS', 'ann': 'CONST_NON_LINEAR_EXPENSES', 'seas': 'CONST_SEASONAL_MONTHLY_BILLS'}
                if cat_type in type_map:
                    target_label = new_cat
                    ALL_CATEGORIES.append(new_cat)
                    new_categories_added.append((new_cat, type_map[cat_type]))
                    # Update shorthand map for the rest of the session
                    while True:
                        code, temp = "", code_idx
                        while temp >= 0:
                            code = chr(temp % 26 + 65) + code
                            temp = temp // 26 - 1
                        code_idx += 1
                        if code not in ['X', 'Y']:
                            break
                    shorthand_map[code] = new_cat
                else:
                    print("Invalid type. Use var, fix, ann, or seas.")
                    continue
            else:
                print("Invalid code or category. Type exactly as shown, X/Y, or NewCat|type.")
                continue
            
            merged_df.at[idx, 'Mapped_Label'] = target_label
            
            # Update Ledger Memory
            # Aggressive wildcard truncation for Amazon items to catch future variations
            if pd.notna(row['Product_Name']):
                clean_desc = desc_prod[:40].strip()
                merchant_string = clean_desc + '*' if not clean_desc.endswith('*') else clean_desc
            else:
                merchant_string = desc_prod[:50] + '*' if len(desc_prod) > 50 else desc_prod
                
            found = False
            for lr in ledger_rows:
                if lr['Merchant_String'] == merchant_string:
                    lr['Target_Label'] = target_label
                    lr['Status'] = '[MAPPED]'
                    lr['Hit_Count'] += 1
                    lr['Last_Seen'] = latest_month
                    found = True
                    break
            if not found:
                ledger_rows.append({'Merchant_String': merchant_string, 'Target_Label': target_label, 'Status': '[MAPPED]', 'Hit_Count': 1, 'Last_Seen': latest_month})
            break

# --- UPDATE HIT COUNTS FOR KNOWN TRANSACTIONS ---
for index, row in merged_df.iterrows():
    if index not in exceptions.index:
        desc = str(row['Product_Name']) if pd.notna(row['Product_Name']) else str(row['Description'])
        for lr in ledger_rows:
            if lr['Merchant_String'] == desc or ('*' in lr['Merchant_String'] and re.match('^' + re.escape(lr['Merchant_String']).replace('\\*', '.*') + '$', desc, re.IGNORECASE)):
                lr['Hit_Count'] += 1
                lr['Last_Seen'] = latest_month
                break

# --- CULL LEDGER ---
max_rows_match = re.search(r'-\s*CONST_MAX_LEDGER_ROWS\s*=\s*(\d+)', constants_raw)
CONST_MAX_LEDGER_ROWS = int(max_rows_match.group(1)) if max_rows_match else 150
protected_cats = CONST_NON_LINEAR_EXPENSES + CONST_STATIC_MONTHLY_BILLS + CONST_SEASONAL_MONTHLY_BILLS + CONST_VARIABLE_SPEND

if len(ledger_rows) > CONST_MAX_LEDGER_ROWS:
    ledger_rows.sort(key=lambda x: (x['Last_Seen'], x['Hit_Count']))
    excess = len(ledger_rows) - CONST_MAX_LEDGER_ROWS
    culled_rows = []
    for lr in ledger_rows:
        if excess > 0 and lr['Target_Label'] not in protected_cats: excess -= 1
        else: culled_rows.append(lr)
    ledger_rows = culled_rows

# --- CALCULATE ACCUMULATORS ---
valid_df = merged_df[~merged_df['Mapped_Label'].str.contains('IGNORE', na=False)].copy()
current_month_dt = datetime.strptime(latest_month, "%Y-%m")
cutoff_date = current_month_dt - relativedelta(months=11)

for cat in CONST_VARIABLE_SPEND:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat]
    if not cat_df.empty:
        if cat not in accumulators: accumulators[cat] = {}
        for _, r in cat_df.iterrows():
            amt, abs_amt = r['Amount'], r['Abs_Amount']
            accumulators[cat][latest_month] = accumulators[cat].get(latest_month, 0.0) + (abs_amt if amt < 0 else -abs_amt)
    if cat in accumulators:
        culled_data = {m: val for m, val in accumulators[cat].items() if m != '_Historical_Average' and datetime.strptime(m, "%Y-%m") >= cutoff_date}
        accumulators[cat] = culled_data
        if culled_data: accumulators[cat]['_Historical_Average'] = sum(culled_data.values()) / len(culled_data)

for cat in CONST_STATIC_MONTHLY_BILLS:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat]
    if not cat_df.empty:
        if cat not in accumulators: accumulators[cat] = {}
        accumulators[cat][latest_month] = accumulators[cat].get(latest_month, 0.0) - cat_df['Amount'].sum()
    if cat in accumulators:
        culled_data = {m: val for m, val in accumulators[cat].items() if datetime.strptime(m, "%Y-%m") >= cutoff_date}
        accumulators[cat] = culled_data

# --- GENERATE PAYLOAD ---
today_str = datetime.now().strftime('%Y-%m-%d')
p_match = re.search(r'_v(\d+)\.txt', os.path.basename(payload_file))
p_ver = int(p_match.group(1)) + 1 if p_match else 1
new_payload_filename = f"Ingestion_Expense_Payload_Chase_{today_str}_v{p_ver}.txt"

payload_out = f"{new_payload_filename}\n================================================================================\n# [START COPY HERE]\n4. INGESTED EXPENSE PAYLOAD\n================================================================================\n"
payload_out += "\n- CONST_STATIC_MONTHLY_BILLS:\n"
for cat in CONST_STATIC_MONTHLY_BILLS:
    if cat in accumulators and latest_month in accumulators[cat]:
        historical_payload[cat] = f"[Amount: ${accumulators[cat][latest_month]:.2f}]"
    payload_out += f"  * {cat}: {historical_payload[cat]}\n"

payload_out += "\n- CONST_VARIABLE_SPEND:\n"
for cat in CONST_VARIABLE_SPEND:
    if cat in accumulators and '_Historical_Average' in accumulators[cat]:
        payload_out += f"  * {cat}: [Historical_Monthly_Average: ${accumulators[cat]['_Historical_Average']:.2f}]\n"
    else:
        payload_out += f"  * {cat}: {historical_payload[cat]}\n"

payload_out += "\n- CONST_NON_LINEAR_EXPENSES:\n"
for cat in CONST_NON_LINEAR_EXPENSES:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat].sort_values('Date')
    if not cat_df.empty: historical_payload[cat] = f"[Due_Month: {cat_df.iloc[-1]['Date'].month}] [Last_Paid_Amount: ${cat_df.iloc[-1]['Abs_Amount']:.2f}]"
    payload_out += f"  * {cat}: {historical_payload[cat]}\n"

payload_out += "\n- CONST_SEASONAL_MONTHLY_BILLS:\n"
for cat in CONST_SEASONAL_MONTHLY_BILLS:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat]
    if not cat_df.empty:
        monthly_sum = -cat_df.groupby(cat_df['Date'].dt.strftime('%b'))['Amount'].sum()
        new_seas = {m: f"${a:.2f}" for m, a in monthly_sum.items()}
        if cat in historical_payload:
            old_seas_str = historical_payload[cat].strip('[]')
            old_seas = {k.strip(): v.strip() for k, v in (item.split(':') for item in old_seas_str.split(',') if ':' in item)}
            old_seas.update(new_seas)
            new_seas = old_seas
        seas_str = ", ".join([f"{m}: {a}" for m, a in new_seas.items()])
        historical_payload[cat] = f"[{seas_str}]"
    payload_out += f"  * {cat}: {historical_payload[cat]}\n"
payload_out += "# [END COPY HERE]\n================================================================================\n"

# --- GENERATE LEDGER ---
l_match = re.search(r'_v(\d+)\.txt', os.path.basename(ledger_file))
l_ver = int(l_match.group(1)) + 1 if l_match else 1
new_ledger_filename = f"Financial_Mapping_Ledger_{today_str}_v{l_ver}.txt"

ledger_out = f"{new_ledger_filename}\n" + "=" * 80 + "\nMERCHANT & PAYEE MAPPING LEDGER\n"
ledger_out += f"Date: {today_str} (Version {l_ver})\nRole: Deterministic Dictionary for CSV/PDF Transaction Mapping & Batch Accumulator\n"
ledger_out += f"File Name: {new_ledger_filename}\n" + "=" * 80 + "\n"
ledger_out += "Merchant_String                     | Target_Label                        | Status         | Hit_Count | Last_Seen\n" + "-" * 115 + "\n"
for lr in ledger_rows: ledger_out += f"{lr['Merchant_String']:<35} | {lr['Target_Label']:<35} | {lr['Status']:<14} | {lr['Hit_Count']:<9} | {lr['Last_Seen']}\n"
ledger_out += "\n" + "-" * 80 + "\n[ACCUMULATOR_LEDGER_JSONS]\n" + "-" * 80 + "\n"
clean_acc = {k: {m: v for m, v in vals.items() if m != '_Historical_Average'} for k, vals in accumulators.items()}
ledger_out += json.dumps(clean_acc, indent=2) + "\n"
ledger_out += "-" * 80 + "\n[PROCESSED_MONTHS_BUFFER]\n" + "-" * 80 + "\n"
if latest_month not in processed_buffer['Chase']: processed_buffer['Chase'].append(latest_month)
ledger_out += json.dumps(processed_buffer, indent=2) + "\n"

# --- SAVE FILES ---
with open(os.path.join(CORE_DIR, new_payload_filename), 'w', encoding='utf-8') as f: f.write(payload_out)
with open(os.path.join(CORE_DIR, new_ledger_filename), 'w', encoding='utf-8') as f: f.write(ledger_out)

# --- SAVE CONSTANTS (IF MODIFIED) ---
if new_categories_added:
    for new_cat, target_list in new_categories_added:
        search_pattern = rf'(-\s*{target_list}.*?\n\s*\(Schema Target:.*?\)\n)'
        replacement = rf'\1    * {new_cat}\n'
        constants_raw = re.sub(search_pattern, replacement, constants_raw, count=1)
    
    c_match = re.search(r'_v(\d+)\.txt', os.path.basename(constants_file))
    c_ver = int(c_match.group(1)) + 1 if c_match else 1
    new_const_filename = f"GEM_Financial_Ingestion_Constants_{today_str}_v{c_ver}.txt"
    with open(os.path.join(CORE_DIR, new_const_filename), 'w', encoding='utf-8') as f:
        f.write(constants_raw)
    print(f"Saved updated Constants: {new_const_filename}")

print("\n" + "="*80)
print(f"SUCCESS: Chase ETL Complete for {latest_month}.")
print(f"Saved updated Ledger: {new_ledger_filename}")
print(f"Saved updated Payload: {new_payload_filename}")
print("="*80 + "\n")

run_mccoy = input("Do you want to automatically execute mccoy_etl.py now? (Y/N): ").strip().upper()
if run_mccoy == 'Y':
    print("\nLaunching McCoy ETL...\n")
    subprocess.run(['python', 'mccoy_etl.py'])
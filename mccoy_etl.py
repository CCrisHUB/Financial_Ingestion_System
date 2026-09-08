#mccoy_etl.py
#"""
#McCoy PDF ETL Pipeline
#Date: 2026-09-08
#Version: 2.0
#Role: Ingests McCoy PDFs, extracts checking withdrawals, maps transactions, and updates accumulators.
#"""
#__version__ = "2.0"
#__date__ = "2026-09-08"

import os
import re
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


# --- DIRECTORY CONFIGURATION ---
CORE_DIR = 'core_files'
INPUT_DIR = 'input_data'

def get_latest_file(directory, prefix, extension):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(extension)]
    if not files: return None
    return sorted(files, key=os.path.getmtime, reverse=True)[0]

print("Initializing McCoy PDF ETL Pipeline...")

# --- FILE DISCOVERY ---
mccoy_file = get_latest_file(INPUT_DIR, 'McCoy', '.pdf')
ledger_file = get_latest_file(CORE_DIR, 'Financial_Mapping_Ledger', '.txt')
payload_file = get_latest_file(CORE_DIR, 'Ingestion_Expense_Payload', '.txt')
constants_file = get_latest_file(CORE_DIR, 'GEM_Financial_Ingestion_Constants', '.txt')

def fatal_error(msg):
    print("\n" + "="*80)
    print(f"FATAL ERROR: {msg}")
    print("="*80 + "\n")
    raise SystemExit("HALT.")

if not mccoy_file: fatal_error("Missing McCoy PDF in input_data folder.")
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
with open(payload_file, 'r', encoding='utf-8') as f:
    for line in f:
        m = re.match(r'^\s*\*\s*([^:]+):\s*(.+)', line)
        if m: historical_payload[m.group(1).strip()] = m.group(2).strip()

# --- EXTRACT PDF TEXT ---
print("Reading PDF...")
import pymupdf
doc = pymupdf.open(mccoy_file)
raw_text = ""
for page in doc:
    raw_text += page.get_text() + "\n"
doc.close()

if len(raw_text.strip()) < 50: fatal_error("Failed to extract text from PDF. It may be an image-based PDF.")

# --- MONTH SYNCHRONIZATION GATE ---
if not processed_buffer.get('Chase'): fatal_error("Chase buffer is empty. You must process Chase before McCoy.")
target_month = processed_buffer['Chase'][-1]

date_match = re.search(r'(?i)Thru:\s*(\d{2}/\d{2}/\d{2})', raw_text)
if not date_match: fatal_error("Could not locate 'Thru: MM/DD/YY' date in PDF header.")
pdf_month = datetime.strptime(date_match.group(1), '%m/%d/%y').strftime('%Y-%m')

if pdf_month != target_month:
    fatal_error(f"Month Mismatch. Expected {target_month} (from Chase), but PDF is {pdf_month}.")
latest_month = target_month

if latest_month in processed_buffer.get('McCoy', []):
    fatal_error(f"Statement for {latest_month} was already processed. Duplicate detected.")

# --- ISOLATE CHECKING & EXTRACT TRANSACTIONS ---
checking_match = re.search(r'(?i)Checking\s+Detail\s+(?:TOTAL|EVERYDAY\+)\s+BUSINESS\s+CHECKING(.*?(?:Summary\s+Detail|Total\s+for\s+this\s+period|$))', raw_text, re.DOTALL)
checking_text = checking_match.group(1) if checking_match else raw_text

pattern = r'^(\d{2}\s?[A-Za-z]{3}\*?)\s+(.*?)\s+(-?\$?\d{1,3}(?:,\d{3})*\.\d{2})\s+(?:\$?\d{1,3}(?:,\d{3})*\.\d{2})$'
transactions = []
for match in re.finditer(pattern, checking_text, re.MULTILINE | re.DOTALL):
    date_str = match.group(1).strip()
    desc = " ".join(match.group(2).split())
    original_desc = desc
    desc = re.sub(r'^(?:Withdrawal ACH|Deposit ACH|Withdrawal|Deposit)\s+', '', desc, flags=re.IGNORECASE).strip()
    desc = re.sub(r'\s+(?:TYPE|ID|DATA|CO|WEB|TEL)[:\s].*', '', desc, flags=re.IGNORECASE).strip()
    desc = re.sub(r'(?i)(Draft\s+\d+)\s+Tracer\s+\d+', r'\1', desc).strip()
    if not desc: desc = original_desc
    amt_str = match.group(3).replace('$', '').replace(',', '')
    transactions.append({'Date': date_str, 'Description': desc, 'Amount': float(amt_str)})

df = pd.DataFrame(transactions)
if df.empty: fatal_error("Regex extraction yielded 0 transactions. Check PDF text format.")

df['Abs_Amount'] = df['Amount'].abs()
df = df[df['Amount'] < 0].copy() # Drop deposits

# --- CHECKSUM VALIDATION GATE ---
print("\n" + "="*80)
user_input = input("Enter the exact Withdrawals line from the PDF (e.g., '8 Withdrawals = 4,453.37'): ").strip()
checksum_match = re.search(r'Withdrawals\s*=\s*(\d{1,3}(?:,\d{3})*\.\d{2})', user_input, re.IGNORECASE)
if not checksum_match: fatal_error("Invalid Checksum format entered.")

expected_total = float(checksum_match.group(1).replace(',', ''))
calculated_total = df['Abs_Amount'].sum()

if abs(expected_total - calculated_total) > 0.02:
    print(f"\n[CHECKSUM FAILURE]\nExpected: ${expected_total:.2f}\nCalculated: ${calculated_total:.2f}")
    fatal_error("Checksum Validation Failed. Extracted math does not match PDF total.")
print("Checksum Passed!")

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

df['Mapped_Label'] = None
for i, row in df.iterrows():
    desc = str(row['Description'])
    amt = row['Abs_Amount']
    mapped = False
    for lr in ledger_rows:
        if lr['Merchant_String'].lower() == desc.lower():
            target = lr['Target_Label']
            if '<' in target or '>' in target: target = evaluate_threshold(amt, target)
            df.at[i, 'Mapped_Label'] = target
            mapped = True
            break
    if not mapped:
        for lr in ledger_rows:
            if '*' in lr['Merchant_String']:
                pattern = '^' + re.escape(lr['Merchant_String']).replace('\\*', '.*') + '$'
                if re.match(pattern, desc, re.IGNORECASE):
                    target = lr['Target_Label']
                    if '<' in target or '>' in target: target = evaluate_threshold(amt, target)
                    df.at[i, 'Mapped_Label'] = target
                    mapped = True
                    break

# --- TERMINAL EXCEPTION GATE ---
exceptions = df[df['Mapped_Label'].isna() | (df['Mapped_Label'] == '[REQUIRES_MANUAL_REVIEW]')].copy()

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
        desc = str(row['Description'])
        amt = row['Amount']
        sign = "-" if amt < 0 else "+"
        
        while True:
            print(f"\nDate: {row['Date']} | Amount: {sign}${abs(amt):.2f}")
            print(f"Desc: {desc}")
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
            
            df.at[idx, 'Mapped_Label'] = target_label
            
            # Ephemeral Check Filter
            is_check = bool(re.search(r"(?i)(?:check|draft)\s+(\d{3,5})", desc))
            if not is_check:
                merchant_string = desc[:50] + '*' if len(desc) > 50 else desc
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
for index, row in df.iterrows():
    if index not in exceptions.index:
        desc = str(row['Description'])
        for lr in ledger_rows:
            if lr['Merchant_String'] == desc or ('*' in lr['Merchant_String'] and re.match('^' + re.escape(lr['Merchant_String']).replace('\\*', '.*') + '$', desc, re.IGNORECASE)):
                lr['Hit_Count'] += 1
                lr['Last_Seen'] = latest_month
                break

# --- CULL LEDGER ---
CONST_MAX_LEDGER_ROWS = 150
protected_cats = CONST_NON_LINEAR_EXPENSES + CONST_STATIC_MONTHLY_BILLS + CONST_SEASONAL_MONTHLY_BILLS
if len(ledger_rows) > CONST_MAX_LEDGER_ROWS:
    ledger_rows.sort(key=lambda x: (x['Last_Seen'], x['Hit_Count']))
    excess = len(ledger_rows) - CONST_MAX_LEDGER_ROWS
    culled_rows = []
    for lr in ledger_rows:
        if excess > 0 and lr['Target_Label'] not in protected_cats: excess -= 1
        else: culled_rows.append(lr)
    ledger_rows = culled_rows

# --- CALCULATE ACCUMULATORS ---
valid_df = df[~df['Mapped_Label'].str.contains('IGNORE', na=False)].copy()
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
        accumulators[cat][latest_month] = cat_df.iloc[-1]['Abs_Amount']

# --- GENERATE FINAL PAYLOAD ---
today_str = datetime.now().strftime('%Y-%m-%d')
p_match = re.search(r'_v(\d+)\.txt', os.path.basename(payload_file))
p_ver = int(p_match.group(1)) + 1 if p_match else 1
new_payload_filename = f"Ingestion_Expense_Payload_FINAL_{today_str}_v{p_ver}.txt"

payload_out = f"{new_payload_filename}\n================================================================================\n# [START COPY HERE]\n4. INGESTED EXPENSE PAYLOAD\n================================================================================\n"
payload_out += "\n- CONST_STATIC_MONTHLY_BILLS:\n"
for cat in CONST_STATIC_MONTHLY_BILLS:
    if cat in accumulators and latest_month in accumulators[cat]:
        historical_payload[cat] = f"[Amount: ${accumulators[cat][latest_month]:.2f}]"
    if cat in historical_payload: payload_out += f"  * {cat}: {historical_payload[cat]}\n"

payload_out += "\n- CONST_VARIABLE_SPEND:\n"
for cat in CONST_VARIABLE_SPEND:
    if cat in accumulators and '_Historical_Average' in accumulators[cat]:
        payload_out += f"  * {cat}: [Historical_Monthly_Average: ${accumulators[cat]['_Historical_Average']:.2f}]\n"

payload_out += "\n- CONST_NON_LINEAR_EXPENSES:\n"
for cat in CONST_NON_LINEAR_EXPENSES:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat].sort_values('Date')
    if not cat_df.empty:
        date_val = str(cat_df.iloc[-1]['Date'])
        month_match = re.search(r'[A-Za-z]{3}', date_val)
        raw_month = month_match.group(0) if month_match else 'Jan'
        int_month = datetime.strptime(raw_month, '%b').month
        historical_payload[cat] = f"[Due_Month: {int_month}] [Last_Paid_Amount: ${cat_df.iloc[-1]['Abs_Amount']:.2f}]"
    if cat in historical_payload: payload_out += f"  * {cat}: {historical_payload[cat]}\n"

payload_out += "\n- CONST_SEASONAL_MONTHLY_BILLS:\n"
for cat in CONST_SEASONAL_MONTHLY_BILLS:
    cat_df = valid_df[valid_df['Mapped_Label'] == cat]
    if not cat_df.empty:
        month_series = cat_df['Date'].str.extract(r'([A-Za-z]{3})', expand=False)
        monthly_sum = cat_df.groupby(month_series)['Abs_Amount'].sum()
        new_seas = {m: f"${a:.2f}" for m, a in monthly_sum.items()}
        if cat in historical_payload:
            old_seas_str = historical_payload[cat].strip('[]')
            old_seas = {k.strip(): v.strip() for k, v in (item.split(':') for item in old_seas_str.split(',') if ':' in item)}
            old_seas.update(new_seas)
            new_seas = old_seas
        seas_str = ", ".join([f"{m}: {a}" for m, a in new_seas.items()])
        historical_payload[cat] = f"[{seas_str}]"
    if cat in historical_payload: payload_out += f"  * {cat}: {historical_payload[cat]}\n"
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
if latest_month not in processed_buffer['McCoy']: processed_buffer['McCoy'].append(latest_month)
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
print(f"SUCCESS: McCoy PDF ETL Complete for {latest_month}.")
print(f"Saved FINAL Ledger: {new_ledger_filename}")
print(f"Saved FINAL Payload: {new_payload_filename}")
print("="*80 + "\n")
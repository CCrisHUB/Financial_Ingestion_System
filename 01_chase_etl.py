#01_chase_etl.py
#"""
#Chase ETL Pipeline
#Date: 2026-09-10
#Version: 3.6.4 (Robust Amazon CSV Header Parsing)
#Role: Ingests Chase CSVs, maps transactions, and updates accumulators.
#"""
__version__ = "3.6.4"
__date__ = "2026-09-10"

import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- DIRECTORY CONFIGURATION ---
CORE_DIR = '00_CORE_Files'
INPUT_DIR = '20_Statements_Current'

def fatal_error(msg):
    print("\n" + "="*80)
    print(f"FATAL ERROR: {msg}")
    print("="*80 + "\n")
    raise SystemExit("HALT.")

# ==============================================================================
# 1. FILE DISCOVERY & PARSING FUNCTIONS
# ==============================================================================

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
    try:
        return os.path.join(directory, sorted(files, key=extract_version, reverse=True)[0])
    except Exception:
        return sorted([os.path.join(directory, f) for f in files], key=os.path.getmtime, reverse=True)[0]

def discover_files():
    while True:
        chase_file = get_latest_file(INPUT_DIR, 'Chase', '.csv')
        amz_order = get_keyword_file(INPUT_DIR, 'order', '.csv')
        amz_refund = get_keyword_file(INPUT_DIR, 'refund', '.csv')
        ledger_file = get_latest_file(CORE_DIR, 'Financial_Mapping_Ledger', '.txt')
        payload_file = get_latest_file(CORE_DIR, 'Ingestion_Expense_Payload_FINAL', '.txt')
        const_file = get_latest_file(CORE_DIR, 'GEM_Financial_Ingestion_Constants', '.txt')

        missing = []
        if not chase_file: missing.append("Chase CSV in 20_Statements_Current")
        if not ledger_file: missing.append("Financial_Mapping_Ledger in 00_CORE_Files")
        if not payload_file: missing.append("Ingestion_Expense_Payload_FINAL in 00_CORE_Files")
        if not const_file: missing.append("GEM_Financial_Ingestion_Constants in 00_CORE_Files")

        if missing:
            print("\n\033[91m[MISSING FILES DETECTED]\033[0m")
            for m in missing: print(f"- {m}")
            retry = input("\033[96mPlace missing files in directories and press ENTER to retry (or 'Q' to quit): \033[0m").strip().upper()
            if retry == 'Q': raise SystemExit("User aborted.")
            continue

        return chase_file, amz_order, amz_refund, ledger_file, payload_file, const_file

def load_constants(filepath):
    with open(filepath, 'r', encoding='utf-8') as f: raw = f.read()
    
    def get_consts(name, text):
        m = re.search(rf'-\s*{name}.*?\n(.*?)(?=\n\s*-|\Z)', text, re.DOTALL)
        if m: return [x.strip().replace('* ', '') for x in m.group(1).split('\n') if x.strip().startswith('*')]
        return []

    categories = {
        'non_linear': get_consts('CONST_NON_LINEAR_EXPENSES', raw),
        'static': get_consts('CONST_STATIC_MONTHLY_BILLS', raw),
        'variable': get_consts('CONST_VARIABLE_SPEND', raw),
        'seasonal': get_consts('CONST_SEASONAL_MONTHLY_BILLS', raw)
    }
    
    sys_labels_raw = get_consts('SYSTEM & EXCLUSION LABELS', raw)
    sys_labels = [re.sub(r'\s*\(.*?\)', '', l).strip() for l in sys_labels_raw]
    
    return raw, categories, sys_labels

def load_ledger(filepath):
    with open(filepath, 'r', encoding='utf-8') as f: raw = f.read()
    ledger_rows = []
    in_table = False
    for line in raw.splitlines():
        if line.startswith('Merchant_String'): in_table = True; continue
        if in_table and line.startswith('---'): continue
        if in_table and line.strip() == '': in_table = False
        if in_table and '|' in line:
            parts = [p.strip() for p in line.rsplit('|', 4)]
            if len(parts) == 5: 
                ledger_rows.append({'Merchant_String': parts[0], 'Target_Label': parts[1], 'Status': parts[2], 'Hit_Count': int(parts[3]), 'Last_Seen': parts[4]})

    acc_match = re.search(r'\[ACCUMULATOR_LEDGER_JSONS\]\n-+\n(.*?)(?:\n-+|$)', raw, re.DOTALL)
    accumulators = json.loads(acc_match.group(1).strip()) if acc_match else {}
    
    buf_match = re.search(r'\[PROCESSED_MONTHS_BUFFER\]\n-+\n(.*?)(?:\n-+|$)', raw, re.DOTALL)
    processed_buffer = json.loads(buf_match.group(1).strip()) if buf_match else {"Chase": [], "McCoy": []}
    
    return ledger_rows, accumulators, processed_buffer

def load_payload(filepath, categories):
    historical_payload = {}
    for cat in categories['static']: historical_payload[cat] = "[Amount: $0.00]"
    for cat in categories['variable']: historical_payload[cat] = "[Historical_Monthly_Average: $0.00]"
    for cat in categories['non_linear']: historical_payload[cat] = "[Due_Month: 1] [Last_Paid_Amount: $0.00]"
    for cat in categories['seasonal']: historical_payload[cat] = "[Pending: $0.00]"

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            m = re.match(r'^\s*\*\s*([^:]+):\s*(.+)', line)
            if m: historical_payload[m.group(1).strip()] = m.group(2).strip()
            
    return historical_payload

# ==============================================================================
# 2. DATA INGESTION & MERGING FUNCTIONS
# ==============================================================================

def ingest_chase(filepath, processed_buffer):
    df = pd.read_csv(filepath)
    if df.empty: fatal_error(f"{filepath} is empty.")
    df['Date'] = pd.to_datetime(df['Transaction Date'])
    df['Abs_Amount'] = pd.to_numeric(df['Amount'], errors='coerce').abs()
    df['Description'] = df['Description'].astype(str).str.replace(r'[^\x20-\x7E]', '', regex=True).str.strip()
    latest_month = df['Date'].dt.to_period('M').max().strftime('%Y-%m')

    if latest_month in processed_buffer.get('Chase', []):
        fatal_error(f"Statement for {latest_month} was already processed. Duplicate detected.")
        
    return df, latest_month

def ingest_amazon(order_file, refund_file):
    def parse_amazon_raw(filepath, is_refund=False):
        if not filepath: return pd.DataFrame()
        try: df = pd.read_csv(filepath)
        except Exception: return pd.DataFrame()
        
        col_map = {str(c).strip().lower(): c for c in df.columns}
        
        if is_refund:
            date_col = col_map.get('refund date', col_map.get('creation date', col_map.get('date')))
            amt_col = col_map.get('refund amount', col_map.get('total amount', col_map.get('total')))
            prod_col = col_map.get('reversal reason', col_map.get('title', col_map.get('product name', col_map.get('items'))))
            ship_col = None
        else:
            date_col = col_map.get('order date', col_map.get('date'))
            amt_col = col_map.get('total owed', col_map.get('total amount', col_map.get('item total', col_map.get('total'))))
            if not amt_col:
                for c_lower, c_orig in col_map.items():
                    if ('total' in c_lower or 'owed' in c_lower or 'amount' in c_lower) and not any(x in c_lower for x in ['discount', 'tax', 'subtotal', 'promotion']):
                        amt_col = c_orig
                        break
            prod_col = col_map.get('product name', col_map.get('title', col_map.get('item name', col_map.get('items'))))
            ship_col = col_map.get('shipment date', col_map.get('ship date'))

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
    if order_file: amazon_frames.append(parse_amazon_raw(order_file, is_refund=False))
    if refund_file: amazon_frames.append(parse_amazon_raw(refund_file, is_refund=True))
    return pd.concat(amazon_frames, ignore_index=True) if amazon_frames else pd.DataFrame()

def merge_amazon_data(chase_df, amazon_df):
    merged_df = chase_df.copy()
    merged_df['Product_Name'] = None

    if not amazon_df.empty:
        for i, chase_row in merged_df.iterrows():
            if 'amazon' in str(chase_row['Description']).lower() or 'amzn' in str(chase_row['Description']).lower():
                amt = chase_row['Abs_Amount']
                c_date = chase_row['Date']
                matches = amazon_df[(amazon_df['Abs_Amount'] == amt) & ((amazon_df['Join_Date'] - c_date).dt.days.abs() <= 5)]
                if not matches.empty:
                    merged_df.at[i, 'Product_Name'] = matches.iloc[0]['Product_Name']
                    amazon_df = amazon_df.drop(matches.index[0])

    if not amazon_df.empty:
        for i, chase_row in merged_df.iterrows():
            if pd.isna(merged_df.at[i, 'Product_Name']) and ('amazon' in str(chase_row['Description']).lower() or 'amzn' in str(chase_row['Description']).lower()):
                amt = chase_row['Abs_Amount']
                c_date = chase_row['Date']
                matches = amazon_df[(amazon_df['Abs_Amount'] == amt) & ((amazon_df['Join_Date'] - c_date).dt.days.abs() <= 21)]
                if not matches.empty:
                    merged_df.at[i, 'Product_Name'] = matches.iloc[0]['Product_Name']
                    amazon_df = amazon_df.drop(matches.index[0])
                    
    return merged_df

# ==============================================================================
# 3. MAPPING & EXCEPTION HANDLING FUNCTIONS
# ==============================================================================

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

def map_transactions(df, ledger_rows):
    df['Mapped_Label'] = None
    for i, row in df.iterrows():
        desc = str(row['Product_Name']) if pd.notna(row['Product_Name']) else str(row['Description'])
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
    return df

def handle_exceptions(df, ledger_rows, categories, latest_month):
    exceptions = df[df['Mapped_Label'].isna() | (df['Mapped_Label'] == '[REQUIRES_MANUAL_REVIEW]')].copy()
    new_categories_added = []
    exception_indices = exceptions.index.tolist()
    
    if exceptions.empty:
        return df, ledger_rows, new_categories_added, exception_indices

    all_cats = categories['non_linear'] + categories['static'] + categories['variable'] + categories['seasonal']
    shorthand_map = {}
    code_idx = 0
    for cat in all_cats:
        while True:
            code, temp = "", code_idx
            while temp >= 0:
                code = chr(temp % 26 + 65) + code
                temp = temp // 26 - 1
            code_idx += 1
            if code not in ['X', 'Y']: break
        shorthand_map[code] = cat
        
    print("\n" + "="*80)
    print(f"MANUAL REVIEW REQUIRED FOR {len(exceptions)} TRANSACTIONS")
    print("="*80)
    print("[X] IGNORE_ANOMALY_OR_DISCRETIONARY       [Y] IGNORE_TRANSFER")
    
    groups = [("NON-LINEAR", categories['non_linear']), ("STATIC BILLS", categories['static']), 
              ("VARIABLE SPEND", categories['variable']), ("SEASONAL", categories['seasonal'])]
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
            
            raw_input = input("Enter Code (e.g. A, AA), X/Y, or NewCat|type: ").strip().rstrip(',.')
            user_input = raw_input.upper()
            
            if user_input == 'X': target_label = '[IGNORE_ANOMALY_OR_DISCRETIONARY]'
            elif user_input == 'Y': target_label = '[IGNORE_TRANSFER]'
            elif user_input in shorthand_map: target_label = shorthand_map[user_input]
            elif user_input in [c.upper() for c in all_cats]: target_label = next(c for c in all_cats if c.upper() == user_input)
            elif '|' in raw_input:
                new_cat, cat_type = raw_input.split('|', 1)
                new_cat, cat_type = new_cat.strip(), cat_type.strip().lower()
                
                type_map = {'var': 'variable', 'fix': 'static', 'ann': 'non_linear', 'seas': 'seasonal'}
                const_map = {'var': 'CONST_VARIABLE_SPEND', 'fix': 'CONST_STATIC_MONTHLY_BILLS', 'ann': 'CONST_NON_LINEAR_EXPENSES', 'seas': 'CONST_SEASONAL_MONTHLY_BILLS'}
                
                if cat_type in type_map:
                    target_label = new_cat
                    categories[type_map[cat_type]].append(new_cat)
                    all_cats.append(new_cat)
                    new_categories_added.append((new_cat, const_map[cat_type]))
                    
                    while True:
                        code, temp = "", code_idx
                        while temp >= 0:
                            code = chr(temp % 26 + 65) + code
                            temp = temp // 26 - 1
                        code_idx += 1
                        if code not in ['X', 'Y']: break
                    shorthand_map[code] = new_cat
                else:
                    print("Invalid type. Use var, fix, ann, or seas.")
                    continue
            else:
                print("Invalid code or category. Type exactly as shown, X/Y, or NewCat|type.")
                continue
            
            df.at[idx, 'Mapped_Label'] = target_label
            
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
            
    return df, ledger_rows, new_categories_added, exception_indices

# ==============================================================================
# 4. STATE MANAGEMENT & ACCUMULATOR FUNCTIONS
# ==============================================================================

def update_hit_counts(df, ledger_rows, latest_month, exception_indices):
    for index, row in df.iterrows():
        if index not in exception_indices:
            desc = str(row['Product_Name']) if pd.notna(row['Product_Name']) else str(row['Description'])
            for lr in ledger_rows:
                if lr['Merchant_String'] == desc or ('*' in lr['Merchant_String'] and re.match('^' + re.escape(lr['Merchant_String']).replace('\\*', '.*') + '$', desc, re.IGNORECASE)):
                    lr['Hit_Count'] += 1
                    lr['Last_Seen'] = latest_month
                    break
    return ledger_rows

def cull_ledger(ledger_rows, categories, sys_labels, const_raw):
    max_rows_match = re.search(r'-\s*CONST_MAX_LEDGER_ROWS\s*=\s*(\d+)', const_raw)
    CONST_MAX_LEDGER_ROWS = int(max_rows_match.group(1)) if max_rows_match else 150
    
    protected_cats = categories['non_linear'] + categories['static'] + categories['seasonal'] + categories['variable'] + ['[IGNORE_TRANSFER]', '[IGNORE_INCOME]']

    if len(ledger_rows) > CONST_MAX_LEDGER_ROWS:
        ledger_rows.sort(key=lambda x: (x['Last_Seen'], x['Hit_Count']))
        excess = len(ledger_rows) - CONST_MAX_LEDGER_ROWS
        culled_rows = []
        for lr in ledger_rows:
            if excess > 0 and lr['Target_Label'] not in protected_cats: excess -= 1
            else: culled_rows.append(lr)
        return culled_rows
    return ledger_rows

def calculate_accumulators(df, accumulators, categories, latest_month, processed_buffer):
    valid_df = df[~df['Mapped_Label'].str.contains('IGNORE', na=False)].copy()
    current_month_dt = datetime.strptime(latest_month, "%Y-%m")
    cutoff_date = current_month_dt - relativedelta(months=11)

    window_months = [m for m in processed_buffer.get('Chase', []) if datetime.strptime(m, "%Y-%m") >= cutoff_date]
    if latest_month not in window_months: 
        window_months.append(latest_month)
    denominator = len(window_months) if window_months else 1

    for cat in categories['variable']:
        cat_df = valid_df[valid_df['Mapped_Label'] == cat]
        if not cat_df.empty:
            if cat not in accumulators: accumulators[cat] = {}
            for _, r in cat_df.iterrows():
                amt, abs_amt = r['Amount'], r['Abs_Amount']
                accumulators[cat][latest_month] = accumulators[cat].get(latest_month, 0.0) + (abs_amt if amt < 0 else -abs_amt)
        if cat in accumulators:
            culled_data = {m: val for m, val in accumulators[cat].items() if m != '_Historical_Average' and datetime.strptime(m, "%Y-%m") >= cutoff_date}
            accumulators[cat] = culled_data
            accumulators[cat]['_Historical_Average'] = sum(culled_data.values()) / denominator if culled_data else 0.0

    for cat in categories['static']:
        cat_df = valid_df[valid_df['Mapped_Label'] == cat]
        if not cat_df.empty:
            if cat not in accumulators: accumulators[cat] = {}
            accumulators[cat][latest_month] = accumulators[cat].get(latest_month, 0.0) - cat_df['Amount'].sum()
        if cat in accumulators:
            culled_data = {m: val for m, val in accumulators[cat].items() if datetime.strptime(m, "%Y-%m") >= cutoff_date}
            accumulators[cat] = culled_data
            
    return accumulators

# ==============================================================================
# 5. FILE GENERATION & ORCHESTRATION
# ==============================================================================

def generate_and_save_files(df, ledger_rows, accumulators, processed_buffer, historical_payload, categories, new_cats, const_raw, latest_month, payload_file, ledger_file, const_file):
    today_str = datetime.now().strftime('%Y-%m-%d')
    valid_df = df[~df['Mapped_Label'].str.contains('IGNORE', na=False)].copy()
    
    # --- PAYLOAD ---
    p_match = re.search(r'_v(\d+)\.txt', os.path.basename(payload_file))
    p_ver = int(p_match.group(1)) + 1 if p_match else 1
    new_payload_filename = f"Ingestion_Expense_Payload_Chase_{today_str}_v{p_ver}.txt"

    payload_out = f"{new_payload_filename}\n================================================================================\n# [START COPY HERE]\n4. INGESTED EXPENSE PAYLOAD\n================================================================================\n"
    
    payload_out += "\n- CONST_STATIC_MONTHLY_BILLS:\n"
    for cat in categories['static']:
        if cat in accumulators and latest_month in accumulators[cat]:
            historical_payload[cat] = f"[Amount: ${accumulators[cat][latest_month]:.2f}]"
        payload_out += f"  * {cat}: {historical_payload[cat]}\n"

    payload_out += "\n- CONST_VARIABLE_SPEND:\n"
    for cat in categories['variable']:
        if cat in accumulators and '_Historical_Average' in accumulators[cat]:
            payload_out += f"  * {cat}: [Historical_Monthly_Average: ${accumulators[cat]['_Historical_Average']:.2f}]\n"
        else:
            payload_out += f"  * {cat}: {historical_payload[cat]}\n"

    payload_out += "\n- CONST_NON_LINEAR_EXPENSES:\n"
    for cat in categories['non_linear']:
        cat_df = valid_df[valid_df['Mapped_Label'] == cat].sort_values('Date')
        if not cat_df.empty: historical_payload[cat] = f"[Due_Month: {cat_df.iloc[-1]['Date'].month}] [Last_Paid_Amount: ${cat_df.iloc[-1]['Abs_Amount']:.2f}]"
        payload_out += f"  * {cat}: {historical_payload[cat]}\n"

    payload_out += "\n- CONST_SEASONAL_MONTHLY_BILLS:\n"
    for cat in categories['seasonal']:
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

    # --- LEDGER ---
    l_match = re.search(r'_v(\d+)\.txt', os.path.basename(ledger_file))
    l_ver = int(l_match.group(1)) + 1 if l_match else 1
    new_ledger_filename = f"Intermediate_Mapping_Ledger_{today_str}_v{l_ver}.txt"

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

    # --- CONSTANTS ---
    if new_cats:
        for new_cat, target_list in new_cats:
            search_pattern = rf'(-\s*{target_list}.*?\n\s*\(Schema Target:.*?\)\n)'
            replacement = rf'\1    * {new_cat}\n'
            const_raw = re.sub(search_pattern, replacement, const_raw, count=1)
        
        c_match = re.search(r'_v(\d+)\.txt', os.path.basename(const_file))
        c_ver = int(c_match.group(1)) + 1 if c_match else 1
        new_const_filename = f"GEM_Financial_Ingestion_Constants_{today_str}_v{c_ver}.txt"
        with open(os.path.join(CORE_DIR, new_const_filename), 'w', encoding='utf-8') as f:
            f.write(const_raw)
        print(f"Saved updated Constants: {new_const_filename}")

    print("\n" + "="*80)
    print(f"SUCCESS: Chase ETL Complete for {latest_month}.")
    print(f"Saved updated Ledger: {new_ledger_filename}")
    print(f"Saved updated Payload: {new_payload_filename}")
    print("="*80 + "\n")

    run_mccoy = input("Do you want to automatically execute 02_mccoy_etl.py now? (Y/N): ").strip().upper()
    if run_mccoy == 'Y':
        print("\nLaunching McCoy ETL...\n")
        subprocess.run(['python', '02_mccoy_etl.py'])

def main():
    print("Initializing Chase ETL Pipeline...")
    
    # 1. Setup & Load
    chase_file, amz_order, amz_refund, ledger_file, payload_file, const_file = discover_files()
    const_raw, categories, sys_labels = load_constants(const_file)
    ledger_rows, accumulators, buffer = load_ledger(ledger_file)
    hist_payload = load_payload(payload_file, categories)
    
    # 2. Ingest & Merge
    chase_df, latest_month = ingest_chase(chase_file, buffer)
    amazon_df = ingest_amazon(amz_order, amz_refund)
    merged_df = merge_amazon_data(chase_df, amazon_df)
    
    # 3. Map & Handle Exceptions
    mapped_df = map_transactions(merged_df, ledger_rows)
    mapped_df, ledger_rows, new_cats, exception_indices = handle_exceptions(mapped_df, ledger_rows, categories, latest_month)
    
    # 4. State Updates
    ledger_rows = update_hit_counts(mapped_df, ledger_rows, latest_month, exception_indices)
    ledger_rows = cull_ledger(ledger_rows, categories, sys_labels, const_raw)
    accumulators = calculate_accumulators(mapped_df, accumulators, categories, latest_month, buffer)
    
    # 5. Output
    generate_and_save_files(
        mapped_df, ledger_rows, accumulators, buffer, hist_payload, 
        categories, new_cats, const_raw, latest_month, 
        payload_file, ledger_file, const_file
    )
    
if __name__ == "__main__":
    main()
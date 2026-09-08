# run_tests.py
import os
import builtins
import chase_etl
import mccoy_etl

print("================================================================================")
print("INITIATING GOLDEN TEST SUITE")
print("================================================================================")

# 1. Dynamically resolve absolute paths based on the script's location
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEST_CORE_DIR = os.path.join(BASE_DIR, 'test_env', 'core_files')
TEST_INPUT_DIR = os.path.join(BASE_DIR, 'test_env', 'input_data')

# Override Directories to use the test environment
chase_etl.CORE_DIR = TEST_CORE_DIR
chase_etl.INPUT_DIR = TEST_INPUT_DIR
mccoy_etl.CORE_DIR = TEST_CORE_DIR
mccoy_etl.INPUT_DIR = TEST_INPUT_DIR

# 2. Mock the input() function to automate the run
original_input = builtins.input
def mock_input(prompt):
    if "execute mccoy_etl.py" in prompt:
        return "N" # Prevent Chase from triggering the real McCoy script via subprocess
    if "Checksum" in prompt:
        print(f"\n[AUTO-RESPONDING TO PROMPT] -> 3 Withdrawals = 330.59")
        return "3 Withdrawals = 330.59"
    return original_input(prompt)
builtins.input = mock_input

# 3. Mock the PDF extractor to return our dummy text
def mock_extract_pdf(filepath):
    print("Simulating PDF Extraction from memory...")
    return """
    Thru: 11/30/24
    TOTAL BUSINESS CHECKING
    06Nov* Withdrawal ACH T-MOBILE -70.29 36,935.80
    12Nov* Draft 1221 Tracer 0510001750 -230.00 14,362.47
    04Nov* Withdrawal ACH AUTHNET GATEWAY -30.30 37,042.28
    """
mccoy_etl.extract_pdf_text = mock_extract_pdf

# 4. Execute the Pipelines
print("\n>>> RUNNING CHASE PIPELINE...")
chase_etl.main()

print("\n>>> RUNNING MCCOY PIPELINE...")
mccoy_etl.main()

print("\n================================================================================")
print("TEST SUITE COMPLETE. Check test_env/core_files for the Golden Output.")
print("================================================================================")
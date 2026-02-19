import pandas as pd
import ast
import os
import re

columns = [
    "Applicant_ID", "Utilization_Ratio", "Debt_to_Income", 
    "Checking_Account_Status", "Loan_Duration", "Age", 
    "Credit_History_Score", "Loan_Purpose", "Default_Status"
]

def convert_mysql_to_csv(input_filename, output_filename):
    # Try common encodings to solve the '0xff' / UTF-16 error
    encodings = ['utf-16', 'utf-8', 'latin-1', 'utf-16-le', 'utf-16-be']
    
    raw_content = None
    for enc in encodings:
        try:
            with open(input_filename, 'r', encoding=enc) as f:
                raw_content = f.read().strip()
                print(f"✅ File successfully read using encoding: {enc}")
                break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if raw_content is None:
        print("❌ Error: Could not decode the file with standard encodings.")
        return

    try:
        # Regex to extract all tuples: (1, 0.2, ...), (2, 0.3, ...)
        tuple_pattern = r'\(\d+,\s*[\d\.]+,.*?\)'
        matches = re.findall(tuple_pattern, raw_content)

        if not matches:
            print(f"❌ No valid data tuples found in {input_filename}.")
            return

        # Convert matches to Python objects
        formatted_string = "[" + ",".join(matches) + "]"
        data_list = ast.literal_eval(formatted_string)

        # Create and Save DataFrame
        df = pd.DataFrame(data_list, columns=columns)
        df.to_csv(output_filename, index=False)
        
        print("="*40)
        print(f"✅ SUCCESS: {len(df)} records converted.")
        print(f"📊 Saved to: {output_filename}")
        print("="*40)
        print(df.head())

    except Exception as e:
        print(f"❌ Processing Error: {e}")

if __name__ == "__main__":
    input_filename = "dataset_dump.sql" 
    output_filename = "final_dataset.csv"
    
    if os.path.exists(input_filename):
        convert_mysql_to_csv(input_filename, output_filename)
    else:
        print(f"❌ File Not Found: {input_filename}")
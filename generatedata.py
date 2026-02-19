import os
import pandas as pd
import numpy as np
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Securely encode the password to handle special characters like '@'
raw_password = os.getenv("DB_PASSWORD")
encoded_password = urllib.parse.quote_plus(raw_password)

DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Create the engine with the encoded password
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}")

def generate_clean_data(total_rows=10000):
    np.random.seed(42)
    batch_size = 1000
    
    for batch_start in range(0, total_rows, batch_size):
        master_entries = []
        for _ in range(batch_size):
            u_type = np.random.choice(['Individual', 'MSME', 'E-Commerce'])
            util = np.random.uniform(0.1, 0.8)
            dti = np.random.uniform(0.1, 0.5)
            risk_weight = (util * 0.5) + (dti * 0.5)
            default = 1 if risk_weight > 0.65 else 0
            
            master_entries.append({
                'utilization_ratio': float(round(util, 4)),
                'debt_to_income': float(round(dti, 4)),
                'credit_history_score': int(1 if util > 0.7 else 0),
                'loan_duration': int(np.random.choice([12, 24, 36, 48])),
                'age': int(np.random.randint(22, 60)),
                'is_unbanked': int(np.random.choice([0, 1], p=[0.7, 0.3])),
                'applicant_type': u_type,
                'default_status': int(default)
            })
        
        with engine.connect() as conn:
            query = text("""
                INSERT INTO tbl_applicants 
                (utilization_ratio, debt_to_income, credit_history_score, loan_duration, age, is_unbanked, applicant_type, default_status) 
                VALUES 
                (:utilization_ratio, :debt_to_income, :credit_history_score, :loan_duration, :age, :is_unbanked, :applicant_type, :default_status)
            """)
            conn.execute(query, master_entries)
            conn.commit()
            
        last_ids = pd.read_sql(text(f"SELECT applicant_id, applicant_type FROM tbl_applicants ORDER BY applicant_id DESC LIMIT {batch_size}"), engine)
        
        msme_data, eco_data, ind_data = [], [], []
        for _, row in last_ids.iterrows():
            aid = int(row['applicant_id'])
            atype = row['applicant_type']
            
            if atype == 'MSME':
                msme_data.append({'applicant_id': aid, 'inventory_turnover_ratio': float(round(np.random.uniform(5, 15), 2)), 'customer_refund_rate': 0.01, 'stockout_frequency': 1, 'sales_growth_velocity': 0.12, 'supplier_payment_delay': 2})
            elif atype == 'E-Commerce':
                eco_data.append({'applicant_id': aid, 'essential_basket_index': 0.80, 'order_regularity_score': 0.9, 'bnpl_repayment_speed': 5, 'cancelled_order_ratio': 0.02})
            else:
                ind_data.append({'applicant_id': aid, 'recharge_buffer': 7, 'emergency_fund_idx': 3.0, 'ott_sub_freq': 0.95})

        if msme_data: pd.DataFrame(msme_data).to_sql('tbl_msme_attributes', con=engine, if_exists='append', index=False)
        if eco_data: pd.DataFrame(eco_data).to_sql('tbl_ecommerce_attributes', con=engine, if_exists='append', index=False)
        if ind_data: pd.DataFrame(ind_data).to_sql('tbl_individual_attributes', con=engine, if_exists='append', index=False)
        
    print(f"Successfully injected {total_rows} rows.")

if __name__ == "__main__":
    generate_clean_data(10000)
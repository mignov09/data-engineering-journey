import pandas as pd
import logging
from sqlalchemy import create_engine

df_csv = pd.read_csv(r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\employees_raw.csv')


df_csv["name"] = df_csv["name"].str.title()
df_csv["department"] = df_csv["department"].str.title()
df_csv["hire_date"]=pd.to_datetime(df_csv["hire_date"], errors='coerce')
df_csv = df_csv.dropna(subset=["salary","name","email"])
logging.info(f"CSV: {len(df_csv)} rows extracted")




api_response = {
    "employees": [
        {"name": "Hank", "department": "Sales",       "salary": 67000},
        {"name": "Iris", "department": "Engineering", "salary": 91000},
        {"name": "Jack", "department": "Sales",       "salary": 75000}
    ]
}
df_employees_api = pd.DataFrame(api_response["employees"])
logging.info(f"API: {len(df_employees_api)} rows extracted")



def read_by_pandas():
    engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/etl_db")
#                                    ↑ driver         ↑ user   ↑password ↑host  ↑port ↑ db
    # pandas read directly from the table as Data frame
    df = pd.read_sql("SELECT * FROM employees", con=engine)
#  show as normal Data frame and length
    print(df)
    logging.info(f"DataFrame read length with {len(df)} rows")
# close engine at the end
    engine.dispose()
    return df

df_read_pandas = read_by_pandas()



def extract_info(*args):
    combine_df = pd.concat([*args], ignore_index=True)
    logging.info(f"Total combined: {len(combine_df)} rows from {len(args)} sources")
    return combine_df

combined = extract_info(df_csv,df_employees_api, df_read_pandas)
print(combined.shape)
print(combined)
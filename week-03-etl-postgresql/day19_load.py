import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)

df = pd.read_csv(r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\combined_data_frame.csv')
df_combined = df[["name", "department", "salary"]].copy()

engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/etl_db")
df_combined.to_sql(
    name="employees_clean", #name from the new table
    con=engine,
    if_exists="replace",
    index=False
)
logging.info(f"{len(df_combined)} rows load into employees_clean table successfully")
engine.dispose()


engine2 = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/etl_db")
df_verify = pd.read_sql("SELECT * FROM employees_clean", con=engine2)
print(df_verify)
engine2.dispose()
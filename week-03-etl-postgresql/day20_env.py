import os
import pandas as pd
import logging
from dotenv import load_dotenv,find_dotenv
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

load_dotenv(find_dotenv())

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise ValueError("missing environment variables in the : .env")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

df = pd.read_csv(
    r"C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\combined_data_frame.csv"
)

df_combined = df[["name", "department", "salary"]].copy()

engine = create_engine(DATABASE_URL)

df_combined.to_sql(
    name="employees_clean",
    con=engine,
    if_exists="replace",
    index=False
)

logging.info(f"{len(df_combined)} rows loaded into employees_clean successfully")

df_verify = pd.read_sql("SELECT * FROM employees_clean", con=engine)
print(df_verify)

engine.dispose()
import pandas as pd
import os
import logging
from dotenv import load_dotenv,find_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
# create a logs folder
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/etl_pipeline.log"),  #save the file in folder
        logging.StreamHandler()]     )   # ← show in console

def read_csv(route):
    try:
        df_csv = pd.read_csv(route)
        df_csv["name"] = df_csv["name"].str.title()
        df_csv["department"] = df_csv["department"].str.title()
        df_csv["hire_date"] = pd.to_datetime(df_csv["hire_date"], errors='coerce')
        df_csv = df_csv.dropna(subset=["salary", "name", "email"])
        logging.info(f"CSV: {len(df_csv)} rows extracted")
        return df_csv

    except FileNotFoundError as e:
        logging.error("No CSV file found")
    except Exception as e:
         logging.error(f"unexpected error found,please check:{e}")







def read_by_pandas():
    try:
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
    except OperationalError as e:
        logging.error(f"check database connection/database connection failed:{e}")
    except ProgrammingError as e:
        logging.error(f"check sql sintax:{e}")
    except Exception as e:
        logging.error(f"unexpected error found,please check:{e}")
def extract_info(*args):
    try:
        combine_df = pd.concat([*args], ignore_index=True)
        logging.info(f"Total combined: {len(combine_df)} rows from {len(args)} sources")
        return combine_df
    except Exception as e:
        logging.error(f"unexpected error found,please check:{e}")
def transform(combine_data_route):
    try:
        df = pd.read_csv(combine_data_route)
        df_combined = df [["name", "department", "salary"]].copy()
        df_combined["name"] = df_combined["name"].str.title()
        df_combined["department"] = df_combined["department"].str.title()
        df_combined["salary"] = df_combined["salary"].astype(float)
        return df_combined
    except FileNotFoundError as e:
        logging.error("No CSV file found")
    except Exception as e:
        logging.error(f"unexpected error found,please check:{e}")
def load_data(df_combined):
    load_dotenv(find_dotenv())

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        raise ValueError("Missing environment variables in .env")

    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    engine = None
    try:
        engine = create_engine(DATABASE_URL)
        df_combined.to_sql("employees_clean", con=engine, if_exists="replace", index=False)
        logging.info(f"{len(df_combined)} rows loaded into employees_clean successfully")
        df_verify = pd.read_sql("SELECT * FROM employees_clean", con=engine)
        print(df_verify)
    except OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Error en Load: {e}")
        raise
    finally:
        if engine is not None:
            engine.dispose()
            logging.info("close connection")



if __name__ == "__main__":
    df_csv_file = read_csv(
        r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\employees_raw.csv')
    api_response = {
        "employees": [
            {"name": "Hank", "department": "Sales", "salary": 67000},
            {"name": "Iris", "department": "Engineering", "salary": 91000},
            {"name": "Jack", "department": "Sales", "salary": 75000}
        ]
    }
    df_employees_api = pd.DataFrame(api_response["employees"])
    logging.info(f"API: {len(df_employees_api)} rows extracted")
    df_read_pandas = read_by_pandas()

    combined = extract_info(df_csv_file, df_employees_api, df_read_pandas)

    combined.to_csv(
        r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\combined_data_frame.csv',
        index=False)

    data_clean = transform(r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\combined_data_frame.csv')

    load_data(data_clean)



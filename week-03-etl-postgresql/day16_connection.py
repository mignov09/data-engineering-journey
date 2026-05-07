import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process.log'),
        logging.StreamHandler()
    ]
)

def get_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="etl_db",
            user="postgres", password="123"
        )
        logging.info(f"Connection successful | version: {conn.server_version}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"connection error: {e}")
        return None  # return None ,if the connection fails

def table_creation(conn):# create a table into etl_db
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        id          SERIAL PRIMARY KEY,
        name        VARCHAR(100),
        department  VARCHAR(50),
        salary      NUMERIC,
        created_at  TIMESTAMP DEFAULT NOW()
    )
""")

    conn.commit()
    cursor.close()
    logging.info(f"table create successfully!!")




def insert_employees(conn,employee_list):
    cursor = conn.cursor()

    rows = [(employee["name"],employee["department"],employee["salary"])for employee in employee_list]

    cursor.executemany("""INSERT INTO employees (name,department,salary) 
             VALUES (%s,%s,%s)""", rows)
    conn.commit()
    cursor.close()
    logging.info(f"{len(rows)} employees retrieved successfully")



def get_info_from_employees(conn):
    cursor = conn.cursor()

    rows = []

    cursor.execute("Select * from employees")
    rows = cursor.fetchall()
    for each_employee in rows:
        print(each_employee)

    cursor.close()
    logging.info(f"{len(rows)}  employees retrieved successfully")




# connection way:
# "postgresql+psycopg2://postgres:password@localhost:5432/etl_db"



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




employees_list = [{"name":"Miguel Fierros","department": "Engineering","salary": 50000},{"name":"Salvador Arroyo","department":"Engineering","salary": 60000},{"name":"Cesar Garcia","department":"Engineering","salary": 100000}]


conn = get_connection()
if conn:
    table_creation(conn)
    insert_employees(conn, employees_list)
    get_info_from_employees(conn)
    read_by_pandas()
    conn.close()
else:
    logging.error("cannot established connection  — pipeline aborted")





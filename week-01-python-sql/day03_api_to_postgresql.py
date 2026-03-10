import requests
from requests.exceptions import ConnectionError, Timeout, RequestException,HTTPError
import json
import logging
import psycopg2
# ─────────────────────────────────────────
# Logger configuration (only once, outside the function)
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process.log'),
        logging.StreamHandler()
    ]
)

def get_json(url: str = "https://jsonplaceholder.typicode.com/users"):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP {response.status_code}: {e}")
        except requests.exceptions.MissingSchema:
            print("invalid URL: missing http:// o https://")
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión: {e}")


def flatten_dict(nested: dict, parent_key: str = "", sep: str = "_") -> dict:
    flat = {}
    for key, value in nested.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat.update(flatten_dict(value, new_key, sep=sep))
        else:
            flat[new_key] = value
    return flat









def get_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="data_engineering",
            user="postgres", password="123"
        )
        logging.info(f"sucessfully conection | version: {conn.server_version}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"connection error: {e}")
        return None  # return None ,if the connection fails


def create_api_users_table(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_users (
            id          INTEGER PRIMARY KEY,
            name        VARCHAR(255)    NOT NULL,
            username    VARCHAR(100)    NOT NULL,
            email       VARCHAR(255)    NOT NULL UNIQUE,
            address_street   VARCHAR(255),
            address_suite    VARCHAR(255),
            address_city     VARCHAR(100),
            address_zipcode  VARCHAR(20),
            address_geo_lat  VARCHAR(50),
            address_geo_lng  VARCHAR(50),
            phone       VARCHAR(50),
            website     VARCHAR(100),
            company_name        VARCHAR(255),
            company_catchPhrase VARCHAR(255),
            company_bs          VARCHAR(255)
        );
    """)

    conn.commit()   #cconfirmed the data inserted on database
    cursor.close()
    logging.info(f"table created sucessfully!!")





def insert_api_users(conn, json_list):
    cursor = conn.cursor()

    rows = [
        (
            user["id"],
            user["name"],
            user["username"],
            user["email"],
            user["address_street"],
            user["address_suite"],
            user["address_city"],
            user["address_zipcode"],
            user["address_geo_lat"],
            user["address_geo_lng"],
            user["phone"],
            user["website"],
            user["company_name"],
            user["company_catchPhrase"],
            user["company_bs"]
        )
        for user in json_list
    ]

    cursor.executemany("""
        INSERT INTO api_users (
            id, name, username, email,
            address_street,address_suite, address_city, address_zipcode,
            address_geo_lat, address_geo_lng,
            phone, website,
            company_name, company_catchPhrase, company_bs
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    """, rows)

    conn.commit()
    cursor.close()
    logging.info(f"{len(rows)} user inserted successfully!!")









def main():
    conn = get_connection()
    if conn is None:        # ✅ stop if there are not connection
        return

    users = get_json()
    flat_users = [flatten_dict(each_user) for each_user in users]
    create_api_users_table(conn)
    insert_api_users(conn, flat_users)
    conn.close()

if __name__ == "__main__":
    main()


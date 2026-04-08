import requests

import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process.log'),
        logging.StreamHandler()
    ]
)



class Extractor:
    base_url = "https://jsonplaceholder.typicode.com/posts"

    def fetch_page(self, page: int, limit: int = 10) -> list:
        response = requests.get(self.base_url, params={"page": page, "limit": limit})
        data = response.json()
        logging.info(f"Page {page} fetched — {len(data)} records")
        return data


    def fetch_all(self, total_pages: int) -> list:
        all_records = []

        for page in range(1, total_pages + 1):
            chunk = self.fetch_page(page)  #with self method  we call other a different class
            if not chunk:
                break
            all_records.extend(chunk)
        logging.info(f"Total extracted: {len(all_records)} records")
        return all_records

class Transformer:

    def clean(self, records: list) -> list:
            # Para cada record:
            each_record  = records
            cleaned_records = []
            for rename_item in each_record :
                rename_item["user_id"] = rename_item.pop("userId")
                rename_item['title'] = rename_item["title"].strip()
                rename_item['body'] = rename_item["body"][:100]
                cleaned_records.append(rename_item)

            return cleaned_records



def get_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="todos",
            user="postgres", password="123"
        )
        logging.info(f"sucessfully conection | version: {conn.server_version}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"connection error: {e}")
        return None  # return None ,if the connection fails

class Loader:
    def __init__(self, conn):
        self.conn = conn


    def create_table(self) -> None:
        cursor = self.conn.cursor()  # ← self.conn en lugar de conn
        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS todos (
                            id          INTEGER PRIMARY KEY,
                            user_id     INTEGER,
                            title       VARCHAR(255),
                            completed        TEXT
                        );
                    """)
        self.conn.commit()
        cursor.close()
        logging.info("Table created successfully!")


    def insert(self, records: list) -> None:
        cursor = self.conn.cursor()

        data = [
            (

                record["id"],
                record["user_id"],
                record["title"],
                record["completed"]

            )
            for record in records
        ]

        cursor.executemany("""
                        INSERT INTO todos (
                            id, user_id, title,completed
                        ) VALUES (%s, %s, %s, %s)
                    """,data)

        self.conn.commit()
        cursor.close()
        logging.info(f"{len(data)} user inserted successfully!!")


if __name__ == "__main__":
    conn = get_connection()

    # ── Guard clause ──────────────────
    if conn is None:
        logging.error("Pipeline aborted: no database connection")
        exit(1)
    # ──────────────────────────────────

    try:
        # 1. Extract
        extractor = Extractor()
        raw_data = extractor.fetch_all(2)

        # 2. Transform
        transformer = Transformer()
        clean_data = transformer.clean(raw_data)

        # 3. load
        loader = Loader(conn)
        loader.create_table()
        loader.insert(clean_data)

        logging.info("Pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

    finally:
        conn.close()        # siempre cierra la conexión
        logging.info("Connection closed")




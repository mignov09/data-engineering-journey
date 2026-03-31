import requests
import json
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





def fetch_page(page: int, limit: int = 10):
    url = "https://jsonplaceholder.typicode.com/todos"
    response = requests.get(url, params={"_page": page, "_limit": limit})
    return response.json()




def fetch_all_chunks(conn,total_pages: int, limit: int = 10) -> None:
    cursor = conn.cursor()
    for page in range(1, total_pages + 1):
        chunk = fetch_page(page, limit)
        if not chunk:
           break
        logging.info(f"Page {page}/{total_pages} - {len(chunk)} records fetched")

        rows = [(record["userId"], record["id"],record["title"],record["completed"])for record in chunk]

        cursor.executemany("""
                           INSERT INTO todos (user_id, id, title, completed
                                                  )
                           VALUES (%s, %s, %s, %s)
                           """, rows)
        conn.commit()
        cursor.close()

        logging.info(f"{len(rows)} records inserted successfully!!")



def main():
    conn = get_connection()
    if conn is None:  # stop if there are not connection
        return

    fetch_all_chunks(conn,20)

if __name__ == "__main__":
    main()
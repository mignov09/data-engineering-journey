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
nested_orders = [
    {
        "customer": {"name": "Ana Lopez", "email": "ana@mail.com"},
        "product": {"name": "Laptop", "price": 1500.00},
        "status": "completed",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Luis Martinez", "email": "luis@mail.com"},
        "product": {"name": "Phone", "price": 800.00},
        "status": "pending",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Pedro Garza", "email": "pedro@mail.com"},
        "product": {"name": "Monitor", "price": 400.00},
        "status": "completed",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Maria Reyes", "email": "maria@mail.com"},
        "product": {"name": "Keyboard", "price": 150.00},
        "status": "cancelled",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Carlos Vega", "email": "carlos@mail.com"},
        "product": {"name": "Laptop", "price": 1200.00},
        "status": "pending",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Sofia Torres", "email": "sofia@mail.com"},
        "product": {"name": "Tablet", "price": 650.00},
        "status": "completed",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Jorge Ruiz", "email": "jorge@mail.com"},
        "product": {"name": "Headphones", "price": 200.00},
        "status": "delivered",
        "created_at": "2026-03-29"
    },
    {
        "customer": {"name": "Laura Mendez", "email": "laura@mail.com"},
        "product": {"name": "Monitor", "price": 450.00},
        "status": "completed",
        "created_at": "2026-03-29"
    }
]

field_map = {
    "customer_name":"customer",  #here we "change" the key both keys the first from
    "product_name":"product",
    "product_price":"amount",
    "status":"status",
    "created_at":"created_at"
}

"""
format needed
[
    {
        "customer": "Ana Lopez",
        "product": "Laptop",
        "amount": 1500.00,
        "status": "completed",
        "created_at": "2026-03-29"
    },
"""




def flatten_dict_nested(nested: dict, parent_key: str = "", sep: str = "_") -> dict:
    flat = {}
    for key, value in nested.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flat.update(flatten_dict_nested(value, new_key, sep=sep))
        else:
            flat[new_key] = value
    return flat

def rename_dict(field_map,original_item_value) :
    rename_value_dict ={}
    for old_key, new_key in field_map.items():
        rename_value_dict[new_key] = original_item_value[old_key]
    return rename_value_dict


def get_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="customer_order",
            user="postgres", password="123"
        )
        logging.info(f"sucessfully conection | version: {conn.server_version}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"connection error: {e}")
        return None  # return None ,if the connection fails


def orders(conn, json_list):
    cursor = conn.cursor()

    rows = [( order["customer"],
              order["product"],
              order["amount"],
              order["status"],
              order["created_at"])
            for order in json_list]

    cursor.executemany("""
        INSERT INTO orders (
            customer, product, amount,
            status,created_at
        ) VALUES ( %s, %s, %s, %s,%s)
    """, rows)

    conn.commit()
    cursor.close()
    logging.info(f"{len(rows)} user inserted successfully!!")

def main():
    conn = get_connection()
    if conn is None:        # stop if there are not connection
        return
    flat_orders = [flatten_dict_nested(order) for order in nested_orders]
    dic_correct_name =  [rename_dict(field_map,rename) for rename in flat_orders]
    orders(conn,dic_correct_name)
    conn.close()

if __name__ == "__main__":
    main()




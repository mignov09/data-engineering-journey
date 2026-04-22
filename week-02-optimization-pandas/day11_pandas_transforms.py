import pandas as pd
import numpy as np

# Dataset de ventas
sales = {
    "order_id":   [1, 2, 3, 4, 5, 6, 7, 8],
    "customer":   ["Alice", "Bob", "Alice", "Carol", "Bob", "Alice", "Carol", "Bob"],
    "product":    ["Laptop", "Phone", "Tablet", "Laptop", "Tablet", "Phone", "Phone", "Laptop"],
    "amount":     [1200, 800, 450, 1200, 450, 800, 800, 1200],
    "quantity":   [1, 2, 3, 1, 2, 1, 2, 1],
    "region":     ["North", "South", "North", "East", "South", "North", "East", "South"]
}

# Dataset de clientes
customers = {
    "customer": ["Alice", "Bob", "Carol", "David"],
    "tier":     ["Gold", "Silver", "Gold", "Bronze"],
    "since":    [2020, 2021, 2019, 2023]
}

df_sales  = pd.DataFrame(sales)
df_customers = pd.DataFrame(customers)
print(df_sales)
#Ejercicio 11.1 — GroupBy básico

# Por cada cliente muestra:
# - total de ventas (sum de amount)
# - número de órdenes (count de order_id)
# - promedio por orden (mean de amount)
# Ordena por total de ventas DESC
resume_customers=df_sales.groupby(by="customer").agg(total_sales=('amount', 'sum'),
qty_orders = ("order_id","count"),mean_order_amount=("amount","mean")).sort_values("total_sales", ascending=False).reset_index()

print(resume_customers)


#Ejercicio 11.2 — GroupBy múltiple

# Agrupa por region Y product
# Muestra total de amount y total de quantity
resume_total = df_sales.groupby(['region', 'product']).agg(
    total_amount=('amount', 'sum'),
    total_qty=('quantity', 'sum')).reset_index()
print(resume_total)

#Ejercicio 11.3 — Merge (JOIN en pandas)

# Une df_sales con df_customers por la columna "customer"
# Muestra todas las ventas con el tier del cliente
# Tipo: LEFT join

left_join=pd.merge(df_sales, df_customers, on="customer", how="left")
print(left_join.groupby('tier').agg(sales_per_customer=("amount","sum")))

#Ejercicio 11.4 — Apply
# Crea una columna nueva "discount" con estas reglas:
# - Si amount >= 1000 → descuento de 10%
# - Si amount >= 500  → descuento de 5%
# - Si amount < 500   → sin descuento (0%)

discount_colums = df_sales.assign(discount=lambda df: df['amount'].apply(lambda x: 0.10 * x if x >= 1000 else(  0.05 * x if x >= 500 else 0 )))

print(discount_colums)




#Ejercicio 11.5 — Pivot Table

# Crea una pivot table que muestre:
# - filas: customer
# - columnas: product
# - valores: sum de amount
# Rellena los NaN con 0



pivot_table_resume=df_sales.pivot_table("amount","customer","product", aggfunc="sum", fill_value=0)
print(pivot_table_resume)
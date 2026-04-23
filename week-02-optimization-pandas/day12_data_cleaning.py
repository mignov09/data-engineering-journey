import pandas as pd
import numpy as np

data = {
    "order_id":  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "customer":  ["Alice", "bob", "Alice", None, "BOB", "Carol", "carol", "Alice", "David", "david"],
    "product":   ["Laptop", "Phone", "Tablet", "Laptop", "phone", "Tablet", "Laptop", "Phone", None, "Tablet"],
    "amount":    [1200, 800, None, 1200, 800, 450, 1200, 800, 450, None],
    "quantity":  [1, 2, 3, 1, 2, 3, 1, 2, 3, 2],
    "date":      ["2024-01-15", "2024-01-20", "2024/02/10", "2024-03-05",
                  "2024-03-15", "2024-04-01", "2024-04-10", "20240505",
                  "2024-06-01", "2024-06-15"]
}

df = pd.DataFrame(data)


#Ejercicio 12.1 — Detectar valores nulos

# Muestra cuántos nulos hay por columna
# Muestra el porcentaje de nulos por columna

qty_nulls = df.isnull().sum()
null_percentage = df.isnull().mean() * 100




#Ejercicio 12.2 — Rellenar y eliminar nulos
# amount nulo → rellena con la mediana de amount
df["amount"]=df["amount"].fillna(df["amount"].median())

# product nulo → rellena con "Unknown"
df["product"]=df["product"].fillna("Unknown")


# customer nulo → elimina esa fila
df = df.dropna(subset=["customer"])







#Ejercicio 12.3 — Normalizar texto
# Estandariza customer y product a Title Case
# Ejemplo: "bob" → "Bob", "phone" → "Phone"
# Elimina duplicados por customer + product
df["customer"]=df["customer"].str.title()
df["product"]=df["product"].str.title()
df = df.drop_duplicates(subset=["customer","product"],keep="first")





#Ejercicio 12.4 — Convertir fechas

# Convierte la columna "date" a datetime
# Tip: usa errors='coerce' para fechas con formato incorrecto
# Muestra cuántas fechas no se pudieron convertir (NaT)

df["date"] = pd.to_datetime(df["date"], errors='coerce')





#Ejercicio 12.5 — Pipeline completo

# Aplica todos los pasos anteriores en secuencia sobre df
# al final imprime df.info() y df.describe()
info = df.info()
resumen =df.describe()

print(info)
print(resumen)
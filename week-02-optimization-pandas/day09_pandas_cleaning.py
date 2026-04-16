import pandas as pd
import numpy as np
data = {
    "id":         [1, 2, 3, 4, 5, 6, 7, 8],
    "name":       ["Alice", "bob", "  Carol  ", "David", "alice", None, "Frank", "Grace"],
    "age":        [25, 30, None, 22, 25, 40, None, 35],
    "salary":     [50000, 60000, 55000, None, 50000, 70000, 48000, 65000],
    "department": ["Engineering", "marketing", "Engineering", "Sales", "Engineering", "HR", "sales", "Marketing"],
    "email":      ["alice@mail.com", "bob@mail.com", "carol@mail.com", "david@mail.com",
                   "alice@mail.com", "frank@mail.com", "frank@mail.com", "grace@mail.com"]
}

df = pd.DataFrame(data)
df_original = df.copy()


#Ejercicio 9.1 — Exploración inicial
# Muestra:
# - las primeras 5 filas
print(df.head(n=5))

# - cuántos nulls hay por columna
print(df.isnull().sum())

# - tipos de datos de cada columna
print(df.dtypes)




#Ejercicio 9.2 — Manejar valores nulos
# - Rellena los nulls de "age" con el promedio de la columna
df["age"] = df["age"].fillna(df["age"].mean())



# - Rellena los nulls de "salary" con la mediana
df["salary"] = df["salary"].fillna(df["salary"].median())


# - Elimina filas donde "name" sea null
df = df.dropna(subset=["name"])



#Ejercicio 9.3 — Estandarizar texto
# - Normaliza "name" → title case (Bob, Carol, Alice)
df["name"]= df["name"].str.title()

# - Normaliza "department" → title case (Engineering, Marketing)
df["department"]= df["department"].str.title()

# - Quita espacios en "name" con strip()
df["name"]= df["name"].str.strip()



#Ejercicio 9.4 — Eliminar duplicados
# - Encuentra filas duplicadas por "name" y "email"
df.duplicated(subset=["name"])
df.duplicated(subset=["email"])
# - Elimina los duplicados manteniendo el primero
df = df.drop_duplicates(subset=["name", "email"])


#Ejercicio 9.5 — Validación final

# - Muestra el DataFrame limpio final
print(df)
# - Imprime cuántas filas tenía antes y después de limpiar
print(f"the shape before cleaning:{df_original.shape}")
print(f"the shape after cleaning:{df.shape}")
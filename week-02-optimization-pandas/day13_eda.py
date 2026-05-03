import pandas as pd
import numpy as np
from numpy.ma.core import mean

data = {
    "employee_id": range(1, 16),
    "name":        ["Alice","Bob","Carol","David","Eve","Frank","Grace",
                    "Hank","Iris","Jack","Karen","Leo","Mia","Nate","Olivia"],
    "department":  ["Engineering","Sales","Engineering","HR","Sales",
                    "Engineering","HR","Sales","Engineering","Sales",
                    "HR","Engineering","Sales","HR","Engineering"],
    "salary":      [95000,62000,88000,54000,71000,102000,49000,67000,
                    91000,75000,52000,110000,68000,None,98000],
    "years":       [5, 3, 7, 2, 4, 9, 1, 3, 6, 5, 2, 12, 4, None, 8],
    "rating":      [4.5,3.8,4.2,3.5,4.0,4.8,3.2,3.9,4.4,4.1,3.6,4.9,3.7,3.8,4.6],
    "remote":      [True,False,True,False,True,True,False,False,
                    True,True,False,True,False,False,True]
}

df = pd.DataFrame(data)


# — Primera inspección
# Muestra: shape, dtypes, info(), describe()
# ¿Cuántos nulos hay? ¿En qué columnas?

print(df.shape)
print(df.dtypes)
print(df.describe())
print(df.isnull().sum())

# — Análisis por departamento
# Por cada departamento muestra:
# - headcount (número de empleados)
# - salario promedio
# - años promedio de experiencia
# - rating promedio
# Ordena por salario promedio DESC

dept_summary = df.groupby("department").agg(
    headcount  = ("employee_id", "count"),
    avg_salary = ("salary",      "mean"),
    avg_years  = ("years",       "mean"),
    avg_rating = ("rating",      "mean")
).sort_values("avg_salary", ascending=False).round(2)

print(dept_summary)

#Filtros y segmentación

# 1. Empleados con salario > promedio general
# 2. Empleados remotos con rating >= 4.5
# 3. Empleados de Engineering con más de 5 años
avg_salary_global = df["salary"].mean()
employees_higher_salary = df[df["salary"] > avg_salary_global]


higher_rating = df.query('remote == True and rating >= 4.5')


year_experience = df.query('department == "Engineering" and years > 5')

print(employees_higher_salary[["name","department","salary"]])
print(higher_rating[["name","remote","rating"]])
print(year_experience[["name","department","years"]])


"""
employees_higher_salary = df.query('salary > @avg_salary')
higher_rating= df.query('rating >= 4.5 ')
year_experience = df.query('years > 5')"""





# — Correlación

# ¿Existe relación entre years y salary?
# Usa df[["salary","years","rating"]].corr()
# Interpreta el resultado en un comentario

correlation =df[["salary","years"]].corr()

# Interpretación esperada en comentario:
# salary vs years: correlación alta (~0.95) → más experiencia = mayor salario
# salary vs rating: correlación media (~0.7) → mejor rating tiende a mayor salario
# years vs rating: correlación media → relación moderada

#Ejercicio 13.5 — Limpieza + resumen final

# Rellena los nulos de salary y years con la mediana de su departamento
# Tip: usa groupby + transform("median")
# Imprime el df limpio final


# Rellena nulos con la mediana del grupo (no del total)
df["salary"] = df["salary"].fillna(df.groupby("department")["salary"].transform("median"))
df["years"]  = df["years"].fillna(df.groupby("department")["years"].transform("median"))  #
print(df)
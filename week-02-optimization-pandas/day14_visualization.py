import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch


# Reutiliza el dataset del Día 13
data = {
    "name":       ["Alice","Bob","Carol","David","Eve","Frank","Grace",
                   "Hank","Iris","Jack","Karen","Leo","Mia","Nate","Olivia"],
    "department": ["Engineering","Sales","Engineering","HR","Sales",
                   "Engineering","HR","Sales","Engineering","Sales",
                   "HR","Engineering","Sales","HR","Engineering"],
    "salary":     [95000,62000,88000,54000,71000,102000,49000,67000,
                   91000,75000,52000,110000,68000,58000,98000],
    "years":      [5,3,7,2,4,9,1,3,6,5,2,12,4,3,8],
    "rating":     [4.5,3.8,4.2,3.5,4.0,4.8,3.2,3.9,4.4,4.1,3.6,4.9,3.7,3.8,4.6],
    "remote":     [True,False,True,False,True,True,False,False,
                   True,True,False,True,False,False,True]
}
df = pd.DataFrame(data)

# — Gráfica de barras
# Salario promedio por departamento (bar chart)
# Eje X: departamento | Eje Y: salario promedio
# Agrega título y etiquetas de ejes

fig, ax = plt.subplots()
average_salary = df.groupby("department").agg(avg_salary = ("salary","mean")).sort_values("avg_salary", ascending=False)


bar_chart=ax.bar( average_salary.index,average_salary["avg_salary"])# index it's like reset_index , convert department into a column and can use indexes
ax.set_title('average salary by department')
ax.set_xlabel('department')#set "department" label into x axis
ax.set_ylabel('avg_salay') #set "average" label into y axis

ax.bar_label(bar_chart, fmt="$%.0f",padding=5)# set a label from the values of avg_salary/label format will"$"






#— Histograma
# Distribución de salarios (histogram)
# Bins = 6, color distinto al default
# Agrega título, etiquetas y una línea vertical en la mediana



fig, ax = plt.subplots()
ax.hist(df["salary"], bins=6, color='steelblue', edgecolor='black')
median = df["salary"].median()
ax.axvline(x=median, color="red", linestyle="--", label=f"Mediana: {median}")# vertical line with median value
ax.set_title("Salary Distribution")
ax.set_xlabel("Salary ($)")
ax.set_ylabel("Number of Employees")
ax.legend() #necessary cause allowed to appear median line



# — Scatter plot

# relation between salary and years
# paint each department

fig, ax = plt.subplots()

color_map = {'Sales': 'red', 'Engineering': 'blue','HR':'green'}
color = df["department"].map(color_map)

ax.scatter(df["years"],df["salary"],c=color)
ax.set_xlabel('years')          # axis X label
ax.set_ylabel('salary')          # axis y label
ax.set_title('Relation between years and salary')# graph title

legend_elements = [ Patch(facecolor='blue',  label='Engineering'),
                    Patch(facecolor='red',   label='Sales'), Patch(facecolor='green', label='HR')#
]
ax.legend(handles=legend_elements)






#— Pie chart

# Distribution by # of employees
# show percentage using autopct='%1.1f%%'
fig, ax = plt.subplots()
categories_group = df.groupby(["department"])["salary"].count()# add
ax.pie(categories_group.values, labels=categories_group.index, autopct='%1.1f%%')
ax.set_title("Percentage of employees by departments")


#— Subplots (dashboard)
fig, axes = plt.subplots(2, 2, figsize=(12, 8))

# ── Bars ───────────────────────────────────────────────
average_salary = df.groupby("department").agg(
    avg_salary=("salary", "mean")
).sort_values("avg_salary", ascending=False)

bar_chart = axes[0, 0].bar(average_salary.index, average_salary["avg_salary"])
axes[0, 0].set_title('Average Salary by Department')
axes[0, 0].set_xlabel('Department')
axes[0, 0].set_ylabel('Avg Salary')
axes[0, 0].bar_label(bar_chart, fmt="$%.0f", padding=5)

# ── Histogram ───────────────────────────────────────────
axes[0, 1].hist(df["salary"], bins=6, color='steelblue', edgecolor='black')
median = df["salary"].median()
axes[0, 1].axvline(x=median, color="red", linestyle="--", label=f"Mediana: {median}")
axes[0, 1].set_title("Salary Distribution")
axes[0, 1].set_xlabel("Salary ($)")
axes[0, 1].set_ylabel("Number of Employees")
axes[0, 1].legend()

# ── Scatter ──────────────────────────────────────────────
color_map = {'Sales': 'red', 'Engineering': 'blue', 'HR': 'green'}
color = df["department"].map(color_map)

axes[1, 0].scatter(df["years"], df["salary"], c=color)
axes[1, 0].set_xlabel('Years')
axes[1, 0].set_ylabel('Salary')
axes[1, 0].set_title('Relation between Years and Salary')

legend_elements = [Patch(facecolor='blue',  label='Engineering'),
                   Patch(facecolor='red',   label='Sales'),
                   Patch(facecolor='green', label='HR')]
axes[1, 0].legend(handles=legend_elements)

# ── Pie ──────────────────────────────────────────────────
categories_group = df.groupby("department")["salary"].count()
axes[1, 1].pie(categories_group.values,
               labels=categories_group.index,
               autopct='%1.1f%%')
axes[1, 1].set_title("Salary by Department")

# ── General ──────────────────────────────────────────────
fig.suptitle("Salary Dashboard", fontsize=16, fontweight='bold')
plt.tight_layout()
plt.savefig("day14_dashboard.png", dpi=150, bbox_inches='tight')
plt.show()







import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

data = {
    "order_id":   range(1, 21),
    "customer":   ["alice","BOB","carol","ALICE","bob","Carol","alice","BOB",
                   "david","carol","ALICE","bob","DAVID","carol","alice",
                   "BOB","david","CAROL","alice","bob"],
    "product":    ["Laptop","Phone","Tablet","Phone","Laptop","Laptop","Tablet",
                   "Phone","Laptop","Phone","Laptop","Tablet","Phone","Laptop",
                   "Phone","Tablet","Laptop","Phone","Tablet","Laptop"],
    "amount":     [1200,800,450,800,1200,1200,450,800,1200,800,
                   1200,450,800,1200,800,450,1200,800,450,None],
    "quantity":   [1,2,3,1,1,2,2,1,1,3,1,2,2,1,1,3,1,2,2,1],
    "date":       ["2024-01-10","2024-01-15","2024-01-20","2024-02-01",
                   "2024-02-10","2024-02-15","2024-03-01","2024-03-10",
                   "2024-03-15","2024-04-01","2024-04-10","2024-04-15",
                   "2024-05-01","2024-05-10","2024-05-15","2024-06-01",
                   "2024-06-10","2024-06-15","2024-07-01","2024-07-10"]
}

df = pd.DataFrame(data)
df_original = df.copy()



dimension = df.shape

null_values_qty= df.isnull().sum()

data_types=df.dtypes

header = df.head()




df["customer"] =df["customer"].str.title()
df["amount"] = df["amount"].fillna(df["amount"].median())
df["date"] = pd.to_datetime(df["date"], errors='coerce')



df["revenue"] = df["amount"] * df["quantity"]
df["month"] = df["date"].dt.month

# Top 3 customers by total revenue
# best seller by qty
# Revenue by month

top_3 = df.groupby("customer")["revenue"].sum().sort_values(ascending=False).head(3)

best_seller = df.groupby("product")["quantity"].sum()
print(best_seller.reset_index().sort_values("quantity", ascending=False))

revenue_by_month = df.groupby("month")["revenue"].sum()
print(revenue_by_month.reset_index().sort_values("month", ascending=True))



# Dashboard with 3 graph:
# 1. Bar chart: revenue by customer
# 2. Line chart: revenue by month
# 3. Pie chart: revenue by product

#1.-Bar chart
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

revenue_by_customer = df.groupby("customer")["revenue"].sum().reset_index()

bar_chart = axes[0].bar(revenue_by_customer["customer"],revenue_by_customer["revenue"].values)
axes[0].set_title('Revenue by Customer')
axes[0].set_xlabel('Customers')
axes[0].tick_params(axis='x', rotation=15)
axes[0].set_ylabel('Revenue')
axes[0].bar_label(bar_chart, fmt="$%.0f", padding=5)
fig.suptitle("day15_mini_project", fontsize=16, fontweight='bold')
plt.tight_layout()




#2.-Line chart:

plot_chart = axes[1].plot(revenue_by_month.index,revenue_by_month.values,marker='o', color='steelblue')
axes[1].set_title('Revenue by month')
month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul"]
axes[1].set_xticks(revenue_by_month.index)
axes[1].set_xticklabels([month_names[m-1] for m in revenue_by_month.index])
axes[1].set_xlabel('month')
axes[1].set_ylabel('revenue')
plt.tight_layout()



# 3. Pie chart:
revenue_by_product = df.groupby("product")["revenue"].sum()
print(revenue_by_product)
axes[2].pie(revenue_by_product.values,labels=revenue_by_product.index,autopct='%1.1f%%')
axes[2].set_title("revenue_by_product")
plt.savefig("day15_mini_project.png", dpi=150, bbox_inches='tight')
plt.show()
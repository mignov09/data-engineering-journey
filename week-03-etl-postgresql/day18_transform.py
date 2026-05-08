import pandas as pd

def transform():
    df = pd.read_csv(r'C:\Users\Dell user\PycharmProjects\data-engineering-journey\week-03-etl-postgresql\combined_data_frame.csv')
    df_combined = df [["name", "department", "salary"]].copy()

    df_combined["name"] = df_combined["name"].str.title()

    df_combined["department"] = df_combined["department"].str.title()


    df_combined["salary"] = df_combined["salary"].astype(float)

    return df_combined


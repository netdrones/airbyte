import pandas as pd

df = pd.read_csv('construction_spending.csv', )
for index, row in df.iterrows():
    for column, value in zip(row.index, row.values):
        print(column, value)
    break

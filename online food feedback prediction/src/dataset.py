import pandas  as pd

df = pd.read_csv("D:/online_Food_feedback_prediction/onlinefoods.csv")
for col in df.columns:
    print(f"Value counts for column '{col}':")
    print(df[col].value_counts())
    print('-' * 50)

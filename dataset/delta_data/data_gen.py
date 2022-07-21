import pandas as pd
df = pd.read_parquet('.\weather_delta.parquet', engine='fastparquet')

df.head()
# print(df)

df["relative_humidity"] = 30
df["temperature"] = 17
df["absolute_humidity"] = 8.009

# print(df.dtypes())

df.to_parquet("delta_gen.parquet",index=False)

from numpy import float64
import pandas as pd
df = pd.read_parquet('.\weather_oct_2020_old.parquet', engine='fastparquet')

df.head()
# print(df.info())

# df["relative_humidity"] = 30
# df["temperature"] = 17
# df["absolute_humidity"] = 8.009

# print(df.info())

df = df.astype({"city_id":str,"date":str,"timestamp": str,"relative_humidity": float64, "temperature": float64})

print(df.info())

df.to_parquet("weather_oct_2020.parquet",index=False)

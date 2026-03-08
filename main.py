import pandas as pd

file = "taxi_zone_lookup.csv"
df = pd.read_csv(file)
print(f"Number of rows in the DataFrame:{len(df)}")

zone_count = (df.groupby("Zone") 
                .agg(location_count=("LocationID", "count"))
                .sort_values("location_count",ascending=False)
                .head(5))
# print(zone_count)

update_df = df.dropna(how = "any")
print(f"Number of rows in the DataFrame after dropping null values:{len(update_df)}")

update_df.to_parquet("taxi_zone_lookup.parquet", engine="pyarrow", index=False )
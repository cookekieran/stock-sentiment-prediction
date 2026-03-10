import pandas as pd
from sqlalchemy import create_engine
from admin import POSTGRES_PASS

connection = f'postgresql+psycopg2://postgres:{POSTGRES_PASS}@localhost:5432/stock_market'

db_engine = create_engine(connection)
query1 = "SELECT * FROM stock_prices"
query2 = "SELECT * FROM futures_prices"

stocks_df = pd.read_sql(query1, db_engine)
futures_df = pd.read_sql(query2, db_engine)

stocks_df.to_parquet("stocks_data.parquet", index=False, compression='snappy')
futures_df.to_parquet("futures_data.parquet", index=False, compression='snappy')

print("Created parquet file")
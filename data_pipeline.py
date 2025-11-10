import requests
import pandas as pd
from datetime import datetime
import sqlite3

# Fetch data
url = "https://fakestoreapi.com/products"
r = requests.get(url)
data = r.json()

# Transform
df = pd.json_normalize(data)
df = df[['id', 'title', 'category', 'price', 'rating.rate', 'rating.count']]
df['fetch_date'] = datetime.now().strftime("%Y-%m-%d")

# Store
conn = sqlite3.connect('sales_data.db')
df.to_sql('daily_sales', conn, if_exists='append', index=False)
conn.close()

print("Data pipeline executed successfully.")

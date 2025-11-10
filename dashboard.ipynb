import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

conn = sqlite3.connect('sales_data.db')
df = pd.read_sql_query("SELECT * FROM daily_sales", conn)

# Average price by category
category_avg = df.groupby('category')['price'].mean().sort_values()
category_avg.plot(kind='bar', title="Average Product Price by Category")
plt.ylabel("Price ($)")
plt.show()

# Rating count by category
rating_count = df.groupby('category')['rating.count'].sum()
rating_count.plot(kind='pie', autopct='%1.1f%%', title="Total Ratings by Category")
plt.show()

conn.close()

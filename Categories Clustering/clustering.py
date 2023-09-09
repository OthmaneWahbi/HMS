import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from transformers import CamembertModel, CamembertTokenizer
import torch
from collections import Counter
import re
import psycopg2
import csv


sql_query = """
SELECT
    products.product_name,
    product_categories.category_name,
    competitors.competitors_name,
    product_pricing_tiers.price_per_unit,
    product_pricing_tiers.min_quantity,
    products.last_updated,
    products.scrapped_at
FROM
    products
INNER JOIN
    product_categories ON products.category_id = product_categories.category_id
INNER JOIN
    competitors ON products.competitor_id = competitors.competitor_id
LEFT JOIN
    product_pricing_tiers ON products.product_id = product_pricing_tiers.product_id;
"""

conn = psycopg2.connect(
    database="hotel_megastore_competition",
    user='postgres',
    password='root',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()
cursor.execute(sql_query)
rows = cursor.fetchall()

csv_file_name = "raw_data.csv"

with open(csv_file_name, mode="w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["product_name", "category_name", "competitors_name", "price_per_unit", "min_quantity", "last_updated", "scrapped_at"])
    for row in rows:
        csv_writer.writerow(row)

cursor.close()
conn.close()


tokenizer = CamembertTokenizer.from_pretrained("camembert-base")
model = CamembertModel.from_pretrained("camembert-base")




df = pd.read_csv(csv_file_name, encoding="ISO-8859-1")

df['product_name'] = df['product_name'].str.lower()
df['category_name'] = df['category_name'].str.lower()

df = df[df['category_name'].str.strip() != '']
df['text'] = df['product_name'] + ' ' + df['category_name']

def get_embedding(text):
    inputs = tokenizer.encode_plus(text, return_tensors='pt', max_length=512, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
    embeddings = outputs.last_hidden_state[0][0].numpy()
    return embeddings

df = df.dropna(subset=['text'])
df['embedding'] = df['text'].apply(get_embedding)
embeddings = np.vstack(df['embedding'].values)
kmeans = KMeans(n_clusters=150, random_state=0).fit(embeddings)
df['cluster'] = kmeans.labels_

def most_common_word(categories):
    category_text = ' '.join(categories)
    words = re.findall(r'\b\w{3,}\b', category_text)
    word_counts = Counter(words)
    if word_counts:
        return max(word_counts, key=word_counts.get)
    else:
        return ""

grouped = df.groupby('cluster')['category_name'].agg(most_common_word)
df['clustered category'] = df['cluster'].map(grouped)
output_path = 'named_clustered.xlsx'
df.to_excel(output_path, index=False, engine='openpyxl')

print("Clustered categories added and saved to", output_path)

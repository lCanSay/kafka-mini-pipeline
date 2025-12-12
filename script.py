import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

url = "https://en.wikipedia.org/wiki/List_of_countries_by_number_of_Internet_users"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

tables = soup.find_all("table")

target_table = None

for table in tables:
    header = table.find("th")
    if header and "Location" in header.text: #i tried to search by class wikitable, but for some reason it doesnt detect it
        target_table = table
        break

if target_table is None:
    raise Exception("Target table not found")

rows = target_table.find_all("tr")

data = []

for row in rows[1:]: 
    cols = row.find_all("td")
    if len(cols) < 7:
        continue

    country_tag = cols[0].find("a")
    country = country_tag.text.strip() if country_tag else cols[0].text.strip()

    data.append({
        "country": country,
        "rate_wb": cols[1].text.strip(),
        "year_wb": cols[2].text.strip(),
        "rate_itu": cols[3].text.strip(),
        "year_itu": cols[4].text.strip(),
        "users_cia": cols[5].text.strip(),
        "year_cia": cols[6].text.strip()
    })

df = pd.DataFrame(data)


df["country"] = df["country"].str.strip().str.lower()
df = df[df["country"] != "world"]

df["rate_wb"] = pd.to_numeric(df["rate_wb"], errors="coerce")
df["rate_itu"] = pd.to_numeric(df["rate_itu"], errors="coerce")

df["year_wb"] = pd.to_numeric(df["year_wb"], errors="coerce")
df["year_itu"] = pd.to_numeric(df["year_itu"], errors="coerce")
df["year_cia"] = pd.to_numeric(df["year_cia"], errors="coerce")

df = df.dropna()

df["year_wb"] = df["year_wb"].astype(int)
df["year_itu"] = df["year_itu"].astype(int)
df["year_cia"] = df["year_cia"].astype(int)


df["users_cia"] = (
    df["users_cia"]
    .astype(str)
    .str.replace(",", "", regex=False)
)

df["users_cia"] = pd.to_numeric(df["users_cia"], errors="coerce")



#kafka

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "bonus_22B030608"
for _, row in df.iterrows():
    producer.send(topic, row.to_dict())

producer.flush()

df.to_csv("cleaned_data.csv", index=False)
print("Saved cleaned_data.csv")

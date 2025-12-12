# Web Scraping → Kafka Mini-Pipeline

## Data Source
URL:  
https://en.wikipedia.org/wiki/List_of_countries_by_number_of_Internet_users

## Description of Data
The dataset contains information about internet usage by country.  
For each country, it includes:
- internet usage rate from the World Bank (WB)
- internet usage rate from the ITU
- total number of internet users from the CIA
- corresponding years for each metric

The data was scraped from a static Wikipedia table using `requests` and `BeautifulSoup`.

## Data Cleaning Steps
The following cleaning operations were applied:
1. Removed the aggregate "World" row.
2. Trimmed whitespace and converted country names to lowercase.
3. Converted internet usage rates to numeric (float).
4. Converted year columns to integers.
5. Removed commas from the number of users and converted it to integer.
6. Removed rows with missing or invalid values.

## Kafka Topic
Topic name:  
`bonus_22B030608`

## Sample Kafka Message
```json
{
  "country": "afghanistan",
  "rate_wb": 18.4,
  "year_wb": 2020,
  "rate_itu": 17.6,
  "year_itu": 2019,
  "users_cia": 7020000,
  "year_cia": 2020
}

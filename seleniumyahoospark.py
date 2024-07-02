from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql.functions import col, to_date
geckodriver_path = r"C:\Users\anous\Downloads\geckodriver-v0.33.0-win64\geckodriver.exe"

firefox_options = webdriver.FirefoxOptions()
driver = webdriver.Firefox()
driver.get('https://finance.yahoo.com/quote/NFLX/history/?period1=1022160600&period2=1719939586')
table_container = driver.find_element(By.CSS_SELECTOR, 'div.table-container.svelte-ewueuo')


tbody = table_container.find_element(By.TAG_NAME, 'tbody')


inner_html=tbody.get_attribute('innerHTML')
soup = BeautifulSoup(inner_html, 'html.parser')


rows = []
for tr in soup.find_all('tr', class_='svelte-ewueuo'):
    cols = [td.text.strip() for td in tr.find_all('td', class_='svelte-ewueuo')]
    rows.append(cols)


spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

# Create a schema for the DataFrame
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Open", StringType(), True),
    StructField("High", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Close", StringType(), True),
    StructField("Adj Close", StringType(), True),
    StructField("Volume", StringType(), True)
])

df = spark.createDataFrame(rows, schema)

df = df.withColumn("Date", to_date(col("Date"), "MMM d, yyyy")) \
       .withColumn("Open", col("Open").cast(FloatType())) \
       .withColumn("High", col("High").cast(FloatType())) \
       .withColumn("Low", col("Low").cast(FloatType())) \
       .withColumn("Close", col("Close").cast(FloatType())) \
       .withColumn("Adj Close", col("Adj Close").cast(FloatType())) \
       .withColumn("Volume", col("Volume").cast(IntegerType()))

df.show()


df.createOrReplaceTempView("stock_data")


spark.sql("SELECT * FROM stock_data").show()
driver.quit()
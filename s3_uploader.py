from pandas import Timestamp
from dotenv import load_dotenv
from io import StringIO
import boto3
import quandl
import csv
import os

load_dotenv()

BUCKET_NAME = 'onwelo-bucket'

# Create dict from locally saved file (csv), that I downloaded from nasdaq Big Mac index (economist_country_codes.csv)
# in order to extract the individual country codes that I later use to upload to s3.
with open('economist_country_codes.csv', mode='r') as inp:
    reader = csv.reader(inp)
    dict_from_csv = {}
    for i in list(reader)[1:]:
        dict_from_csv[i[0].split('|')[0]] = i[0].split('|')[1]

# I use quandl in order to get data directly from nasdaq site instead of downloading it locally to computer.
quandl.ApiConfig.api_key = os.getenv("api_key")

# Initialisation of resource which I will later use to transfer data to s3.
s3_resource = boto3.resource('s3',
                             aws_access_key_id=os.getenv("access_key"),
                             aws_secret_access_key=os.getenv("secret_access_key"))

top = []
# Getting data for every country by looping through their codes and sending them to s3.
for key, value in dict_from_csv.items():
    csv_buffer = StringIO()
    data = quandl.get(f'ECONOMIST/BIGMAC_{value}')

    # I checked which states had the highest Big Mac index in July 2021.
    # This is not an optimal solution to this problem, but the advantage is that I don't create a new script
    # in which I would have to reload everything, but instead I use an existing loop to write data to s3.
    if Timestamp('2021-07-31 00:00:00') in data.index.tolist():
        top.append((data.loc['2021-07-31', 'local_price']/data.loc['2021-07-31', 'dollar_ex'], key))

    data.to_csv(csv_buffer)
    s3_resource.Object(BUCKET_NAME, f'economist_bigmac_{key}.csv').put(Body=csv_buffer.getvalue())

top.sort(key=lambda pair: pair[0], reverse=True)
print(top[:5])

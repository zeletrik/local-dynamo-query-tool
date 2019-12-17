from __future__ import print_function
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
import argparse

parser = argparse.ArgumentParser(description='Help for local DynamoDB Query tool')
parser.add_argument("--endpoint", default="http://localhost:8000", type=str, help="The endpoint where the Dynamo DB is located")
parser.add_argument("--table", type=str, required=True, help="The table to query")
parser.add_argument("--key", default="", type=str, help="Access key id")
parser.add_argument("--secret", default="", type=str, help="Access key seret")
parser.add_argument("--region", default="local", type=str, help="Region")
parser.add_argument("--cond", default="", type=str, help="Partition key and value to query, if not peresent then all the items will return")
args = parser.parse_args()

endpoint = args.endpoint
table = args.table
key = args.key
secret = args.secret
region = args.region
condition = args.cond

print("Welcome to the Dynamo DB Query tool, the query shall begin for the DynamoDB located in", endpoint, "and table:", table)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource("dynamodb",
                          aws_access_key_id=key,
                          aws_secret_access_key=secret,
                          region_name=region,
                          endpoint_url=endpoint)

table = dynamodb.Table(table)

if condition is "":
    print("Query all items")
    response = table.scan()
else:
    data = condition.split("=")
    part = data[0]
    value = data[1]
    print("Query table for partition", part, "with value", value)
    try:
       value = int(value)
    except ValueError:
        print ("Value cannot cased to Decimal, let's proceed with text")
  
    response = table.query(KeyConditionExpression=Key(part).eq(value))

for i in response["Items"]:
    print(i)

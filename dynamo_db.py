import boto3
from botocore.exceptions import ClientError

from input_data import items
from utils import Demo

# If you are working on an EC2 instance, and it has proper
# permissions (namely DynamoDB access permissions: read, write, etc.)
# the setup session call is NOT needed. Use it only when outside AWS
# boto3.setup_default_session(profile_name='soldev-training')

dynamo_resource = boto3.resource('dynamodb', region_name='us-east-1')  # or choose a different region, i.e. us-west-1

new_demo = Demo(dynamo_resource)

# Rename the table as you want, i.e. MyDemoTable (make sure the name is unique!)
# Run once to create the table, then comment it out
# new_demo.create_table(table_name='workshop')

[new_demo.add_item(table_name='workshop', **item) for item in items]

# Retrive a single record using key schema: CustomerID and OrderID
get_schema_key = new_demo.get_item(table_name='workshop', customerID='111222', orderID=331)
print(f"Getting item by primary key: {get_schema_key}\n")  # newlines are added for VS code only, Pycharm is OK w/o them

# Get item doesn't work if the primary key is incomplete
try:
    get_incomplete_key = new_demo.get_item(table_name='workshop', transactionID='tr120')
except ClientError as e:
    print(f"Incomplete schema key error: {e}")

# Retrive all records with the schema partition key: customerID
query_customers = new_demo.query_items(partition_key_name='customerID', partition_key_value='111222')
print(f"All records for 111222 customer: {query_customers}\n")

# Retrive all records with a GSI partition key: transactionID
# transactuinID is a global secondary index
query_customers = new_demo.query_items(partition_key_name='transactionID', partition_key_value='tr120', index_name='transactionID-index')
print(f"All records for 111222 customer: {query_customers}\n")

# Retrive all records with a given partition key: CustomerID and LSI: orderSum
query_customers = new_demo.query_items(partition_key_name='customerID', partition_key_value='111222',
                                       range_key_name='orderSum', range_key_bounds=(7,10), index_name='orderSum-index')
print(f"All records for 111222 customer LSI key: {query_customers}\n")

# Implement Scan here
# scan_all_records = new_demo.scan_all()
# print(f"All records in table: {scan_all_records}\n")

import logging
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()

class Demo:
    """Encapsulates an Amazon DynamoDB table of demo data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.resource = dyn_resource
        self.table = None

    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store demo data.
        The table uses the release customer ID as the partition key and the
        order ID as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'customerID', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'orderID', 'KeyType': 'RANGE'},  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'customerID', 'AttributeType': 'S'},
                    {'AttributeName': 'orderID', 'AttributeType': 'N'},
                    {'AttributeName': 'orderSum', 'AttributeType': 'N'},
                    {'AttributeName': 'transactionID', 'AttributeType': 'S'},
                ],
                LocalSecondaryIndexes=[
                    {
                        'IndexName': 'orderSum-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'customerID',
                                'KeyType': 'HASH'},  # Partition key for LSI
                            {
                                'AttributeName': 'orderSum',
                                'KeyType': 'RANGE'
                            },
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        }
                    },
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'transactionID-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'transactionID',  # new partition key
                                'KeyType': 'HASH'
                            },
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL',
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 10,
                            'WriteCapacityUnits': 10
                        }
                    },
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def set_table(self, table_name):
        if not self.table:
            self.table = self.resource.Table(table_name)

    def add_item(self, table_name=None, **kwargs):
        """
        Adds an item to the table.

        :param table_name: if method is used on an existing table, table_name should be provided
        :param kwargs: dictionary attribute name/attribute value
        """
        self.set_table(table_name=table_name)

        try:
            self.table.put_item(
                Item=kwargs)
        except ClientError as err:
            logger.error(
                "Couldn't add an item %s to table %s. Here's why: %s: %s",
                kwargs, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_item(self, table_name=None, **kwargs):
        """
        Gets item data from the table.
        :param table_name: if method is used on an existing table, table_name should be provided
        :return: The data about the requested item.
        """
        self.set_table(table_name=table_name)

        try:
            response = self.table.get_item(Key=kwargs)
        except ClientError as err:
            logger.error(
                "Couldn't get item %s from table %s. Here's why: %s: %s",
                kwargs, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    def query_items(self, partition_key_name, partition_key_value,
                    range_key_name=None, range_key_bounds=None,
                    index_name=None, table_name=None):
        """
        Queries for customers that had transactions within certain sum of money (OrderSum).

        :param table_name: table where query is run
        :param range_key_bounds: list of two bounds, i.e. [7, 10]
        :param range_key_name: range or sort key name
        :param index_name: often ends with -index
        :param partition_key_value: use a partition key
        :param partition_key_name: value to query
        :return: The list of items that were sold.
        """
        self.set_table(table_name=table_name)

        additional_attr = {}
        key_condition_expr = {'KeyConditionExpression': Key(partition_key_name).eq(partition_key_value)}
        if index_name:
            additional_attr = {'IndexName': index_name}
        if range_key_name:
            key_condition_expr['KeyConditionExpression'] = key_condition_expr['KeyConditionExpression'] \
                                                           & Key(range_key_name).between(range_key_bounds[0],
                                                                                         range_key_bounds[1])
        try:
            response = self.table.query(
                **additional_attr,
                **key_condition_expr
            )
        except ClientError as err:
            logger.error(
                "Couldn't query for items released for %s. Here's why: %s: %s", partition_key_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    def scan_all(self, table_name=None):
        """
        Returns all items.

        :return: The list of items.
        """
        pass

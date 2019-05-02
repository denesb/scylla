# Tests for the basic table operations: CreateTable, DeleteTable, DescribeTable.

import pytest
from botocore.exceptions import ClientError
import re

# Utility function for create a table with a given name and some valid
# schema.. This function initiates the table's creation, but doesn't
# wait for the table to actually become ready.
def create_table(dynamodb, name):
    return dynamodb.create_table(
        TableName=name,
        BillingMode='PAY_PER_REQUEST',
        KeySchema=[
            {
                'AttributeName': 'p',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'c',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'p',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'c',
                'AttributeType': 'S'
            },
        ],
    )

# Utility function for creating a table with a given name, and then deleting
# it immediately, waiting for these operations to complete. Since the wait
# uses DescribeTable, this function requires all of CreateTable, DescribeTable
# and DeleteTable to work correctly.
# Note that in DynamoDB, table deletion takes a very long time, so tests
# successfully using this function are very slow.
def create_and_delete_table(dynamodb, name):
    table = create_table(dynamodb, name)
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    table.delete()
    table.meta.client.get_waiter('table_not_exists').wait(TableName=name)

##############################################################################

# Test creating a table, and then deleting it, waiting for each operation
# to have completed before proceeding. Since the wait uses DescribeTable,
# this tests requires all of CreateTable, DescribeTable and DeleteTable to
# function properly in their basic use cases.
# Unfortunately, this test is extremely slow with DynamoDB because deleting
# a table is extremely slow until it really happens.
def test_create_and_delete_table(dynamodb):
    create_and_delete_table(dynamodb, 'alternator_test')

# DynamoDB documentation specifies that table names must be 3-255 characters,
# and match the regex [a-zA-Z0-9._-]+. Names not matching these rules should
# be rejected, and no table be created.
def test_create_table_unsupported_names(dynamodb):
    from botocore.exceptions import ParamValidationError, ClientError
    # Intererstingly, the boto library tests for names shorter than the
    # minimum length (3 characters) immediately, and failure results in
    # ParamValidationError. But the other invalid names are passed to
    # DynamoDB, which returns an HTTP response code, which results in a
    # CientError exception.
    with pytest.raises(ParamValidationError):
        create_table(dynamodb, 'n')
    with pytest.raises(ParamValidationError):
        create_table(dynamodb, 'nn')
    with pytest.raises(ClientError, match='at most 255 characters long'):
        create_table(dynamodb, 'n' * 256)
    with pytest.raises(ClientError, match='validation error detected'):
        create_table(dynamodb, 'nyh@test')

# On the other hand, names following the above rules should be accepted. Even
# names which the Scylla rules forbid, such as a name starting with .
def test_create_and_delete_table_non_scylla_name(dynamodb):
    create_and_delete_table(dynamodb, '.alternator_test')


# DescribeTable error path: trying to describe a non-existent table should
# result in a ResourceNotFoundException.
def test_describe_table_non_existent_table(dynamodb):
    with pytest.raises(ClientError, match='ResourceNotFoundException') as einfo:
        dynamodb.meta.client.describe_table(TableName='non_existent_table')
    # As one of the first error-path tests that we wrote, let's test in more
    # detail that the error reply has the appropriate fields:
    response = einfo.value.response
    print(response)
    err = response['Error']
    assert err['Code'] == 'ResourceNotFoundException'
    assert re.match(err['Message'], 'Requested resource not found: Table: non_existent_table not found')

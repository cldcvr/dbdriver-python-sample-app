''' dynamodb interface '''
import os
import inspect

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key


class Dynamo(object):
    ''' db '''
    def __init__(self, region):
        self.endpoints = {
            "get": "GetItem",
            "put": "PutItem",
            "query": "Query",
            "update": "UpdateItem",
            "delete": "DeleteItem",
            "batchGet": "BatchGetItem",
            "batchWrite": "BatchWriteItem"
        }
        self._isSpanner = False
        self.endpoint = None
        self._config = Config(region_name=region)
        self._check_spanner()

    def _check_spanner(self):
        if os.getenv("DYNAMO_LOCAL_HOST", None):
            self._isSpanner = True

    def _get_client_kwargs(self, action):
        kwargs = {}
        if self._isSpanner:
            kwargs["endpoint_url"] = "http://{}:{}/v1/{}".format(
                os.environ['DYNAMO_LOCAL_HOST'],
                os.environ['DYNAMO_LOCAL_PORT'],
                self.endpoints[action]
            )
        return kwargs

    def _get_client(self, action):
        kwargs = self._get_client_kwargs(action)
        return boto3.client("dynamodb", **kwargs)

    def get(self, table_name, key):
        client = self._get_client(inspect.stack()[0][3])
        data = client.get_item(TableName=table_name, Key=key)
        if not data.get("Item"):
            return {}
        return data.get("Item")

    def put(self, table_name, item):
        client = self._get_client(inspect.stack()[0][3])
        data = client.put_item(TableName=table_name, Item=item)
        if not data.get("Item"):
            return {}
        return data.get("Item")

    def query(self, **kwargs):
        client = self._get_client(inspect.stack()[0][3])
        data = client.query(**kwargs)
        if not data.get("Items"):
            return {}
        return data.get("Items")

    def update(self, table_name, **kwargs):
        kwargs["TableName"] = table_name
        kwargs["ReturnValues"] = "ALL_NEW"
        client = self._get_client(inspect.stack()[0][3])
        data = client.update_item(**kwargs)
        if not data.get("Attributes"):
            return {}
        return data.get("Attributes")

    def delete(self, table_name, **kwargs):
        kwargs["TableName"] = table_name
        kwargs["ReturnValues"] = "ALL_OLD"
        client = self._get_client(inspect.stack()[0][3])
        data = client.delete_item(**kwargs)
        if not data.get("Attributes"):
            return {}
        return data.get("Attributes")

    def batchGet(self, **kwargs):
        client = self._get_client(inspect.stack()[0][3])
        data = client.batch_get_item(**kwargs)
        if not data.get("Responses"):
            return {}
        return data.get("Responses")

    def batchWrite(self, **kwargs):
        client = self._get_client(inspect.stack()[0][3])
        data = client.batch_write_item(**kwargs)
        return data.get("UnprocessedItems")


def get_update_args(values):
    expression_values = {}
    update_exp = "SET "
    i = 0
    for k, v in values.items():
        _type = "S"
        if isinstance(v, int):
            _type = "N"
            v = str(v)
        if isinstance(v, list):
            _type = "L"
        if len(k.split(".")) > 1:
            exp = k.split(".")[1]
        else:
            exp = k
        update_exp += k + "=:" + exp
        expression_values[":"+exp] = {_type: v}
        if i == 0 and i < len(values)-1:
            update_exp += ", "
        i += 1
    return update_exp, expression_values


db = Dynamo(os.getenv("AWS_REGION", "ap-southeast-1"))

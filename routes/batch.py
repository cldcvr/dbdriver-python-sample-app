import json

from botocore.exceptions import ClientError

from flask import request, Response
from flask_restful import Resource
from flask_restful import reqparse

from ecommercy.utils.utils import serialize, deserialize
from ecommercy.db.db import db

class BatchGet(Resource):
    ''' BatchGet '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        args = {}

        for table, items in body["request_items"].items():
            keys = []
            for item in items:
                keys.append(serialize(item))
            args[table] = {"Keys": keys}
        kwargs = {"RequestItems": args}
        try:
            data = db.batchGet(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        resp = {}
        for k, vals in data.items():
            resp[k] = [deserialize(v) for v in vals]
        return resp, 200


class BatchWrite(Resource):
    ''' BatchGet '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        for table_name, reqs in body["request_items"].items():
            kwargs = {table_name: []}
            for req_type, items in reqs.items():
                l = []
                for item in items:
                    if req_type == "PutRequest":
                        l.append({req_type: {"Item": serialize(item)}})
                    else:
                        l.append({req_type: {"Key": serialize(item)}})
                kwargs[table_name].extend(l)
        try:
            items = db.batchWrite(RequestItems=kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        
        if len(items) != 0:
            return Response(response=json.dumps({"message": "Unprocessed items {}".format(json.dumps(items))}),
                status=404, mimetype='application/json')
        return {"message": "all items processed"}, 201


def get_items(start_num, num_items):
    """
    Generate a sequence of dynamo items
    :param start_num: Start index
    :type start_num: int
    :param num_items: Number of items
    :type num_items: int
    :return: List of dictionaries
    :rtype: list of dict
    """
    result = []
    for i in range(start_num, start_num+num_items):
        result.append({'pk': {'S': 'item{0}'.format(i)}})
    return result


def create_batch_write_structure(table_name, start_num, num_items):
    """
    Create item structure for passing to batch_write_item
    :param table_name: DynamoDB table name
    :type table_name: str
    :param start_num: Start index
    :type start_num: int
    :param num_items: Number of items
    :type num_items: int
    :return: dictionary of tables to write to
    :rtype: dict
    """
    return {
        table_name: [
            {'PutRequest': {'Item': item}}
            for item in get_items(start_num, num_items)
        ]
    }


        # args = {}
        # for table, requests in body["request_items"]:
        #     keys = []
        #     for reqs in requests: 

        #         keys.append(serialize(item))
        #     args[table] = {"Keys": keys}
        # kwargs = {"RequestItems": args}
        # try:
        #     items = db.batchGet(**kwargs)
        # except ClientError as e:
        #     return Response(
        #         response=json.dumps({"error": str(e)}), 
        #         status=500, mimetype='application/json')
        
        # if len(items) == 0:
        #     return Response(response=json.dumps({"message": "Not found"}),
        #         status=404, mimetype='application/json')
        # users = [deserialize(item) for item in items]
        # return users[0], 200
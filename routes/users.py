import json

from botocore.exceptions import ClientError

from flask import request, Response
from flask_restful import Resource

from ecommercy.utils.utils import serialize, deserialize
from ecommercy.db.db import db


class User(Resource):
    ''' Users resource '''
    TableName = "users"

    def get(self):
        body = request.get_json()
        try:
            user = db.get(self.TableName, key=serialize(body))
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(user.items()) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        return deserialize(user), 200

    def post(self):
        user = request.get_json()
        try:
            db.put(self.TableName, item=serialize(user))
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        return user, 201


class QueryUsers(Resource):
    ''' QueryUsers '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        kwargs = {
            "TableName": self.TableName,
            "KeyConditionExpression": "first_name = :a",
            "ExpressionAttributeValues": {
                ":a": {
                    "S": body["first_name"],
                }
            }
        }
        try:
            items = db.query(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        users = [deserialize(item) for item in items]
        return users, 200


class QueryUserID(Resource):
    ''' QueryUserID '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        kwargs = {
            "TableName": self.TableName,
            "KeyConditionExpression": "first_name = :a and email = :b",
            "ExpressionAttributeValues": {
                ":a": {
                    "S": body["first_name"]
                },
                ":b": {
                    "S": body["email"]
                }
            }
        }
        try:
            items = db.query(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        users = [deserialize(item) for item in items]
        return users[0], 200


class QueryUserBegins(Resource):
    ''' QueryUserBegins '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        kwargs = {
            "TableName": self.TableName,
            "KeyConditionExpression": "first_name = :a and begins_with(email, :b)",
            "ExpressionAttributeValues": {
                ":a": {
                    "S": body["first_name"]
                },
                ":b": {
                    "S": body["email"]
                }
            }
        }
        try:
            items = db.query(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        users = [deserialize(item) for item in items]
        return users, 200


class QueryIndexAll(Resource):
    ''' QueryIndexAll '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        kwargs = {
            "TableName": self.TableName,
            "IndexName": "country_age_index",
            "KeyConditionExpression": "country = :a",
            "ExpressionAttributeValues": {
                ":a": {
                    "S": body["country"]
                }
            }
        }
        try:
            items = db.query(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        users = [deserialize(item) for item in items]
        return users, 200


class QueryIndexRange(Resource):
    ''' QueryIndexRange '''
    TableName = "users"

    def post(self):
        body = request.get_json()
        kwargs = {
            "TableName": self.TableName,
            "IndexName": "country_age_index",
            "KeyConditionExpression": "country = :a and age BETWEEN :t1 and :t2",
            "ExpressionAttributeValues": {
                ":a": {
                    "S": body["country"]
                },
                ":t1": {"N": str(body["age"]["from"])},
                ":t2": {"N": str(body["age"]["to"])},
            }
        }
        try:
            items = db.query(**kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(items) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        users = [deserialize(item) for item in items]
        return users, 200


class UpdateOrPutItemID(Resource):
    ''' UpdateItemID '''
    TableName = "users"

    def put(self):
        body = request.get_json()
        kwargs = {
            "Key": {
                "first_name": {"S": body["first_name"]},
                "email": {"S": body["email"]},
            },
            "UpdateExpression": "SET country = :val",
            "ExpressionAttributeValues": {
                ":val": {"S": body["country"]},
            }
        }
        try:
            user = db.update(self.TableName, **kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        if len(user) == 0:
            return Response(response=json.dumps({"message": "Not found"}),
                status=404, mimetype='application/json')
        return deserialize(user), 201


class UpdateItemIfExists(Resource):
    ''' UpdateItemIfExists '''
    TableName = "users"

    def put(self):
        body = request.get_json()
        kwargs = {
            "Key": {
                "first_name": {"S": body["first_name"]},
                "email": {"S": body["email"]},
            },
            "ConditionExpression": "attribute_exists(first_name)",
            "UpdateExpression": "SET last_name = :a",
            "ExpressionAttributeValues": {
                ":a": {"S": body["last_name"]},
            }
        }
        try:
            users = db.update(self.TableName, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                return Response(
                    response=json.dumps({"error": "Key invalid"}), 
                    status=404, mimetype='application/json')
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        return deserialize(users), 201


class UpdateItemCondition(Resource):
    ''' UpdateItemCondition '''
    TableName = "users"

    def put(self):
        body = request.get_json()
        kwargs = {
            "Key": {
                "first_name": {"S": body["first_name"]},
                "email": {"S": body["email"]},
            },
            "UpdateExpression": "SET age = :b",
            "ConditionExpression": "age < :b",
            "ExpressionAttributeValues": {
                ":b": {"N": str(body["age"])},
            }
        }
        try:
            users = db.update(self.TableName, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return Response(
                    response=json.dumps({"message": "Invalid input for operation"}), 
                    status=400, mimetype='application/json')
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        return deserialize(users), 201


class DeleteItem(Resource):
    ''' DeleteItem '''
    TableName = "users"

    def delete(self):
        body = request.get_json()
        kwargs = {
            "Key": {
                "first_name": {"S": body["first_name"]},
                "email": {"S": body["email"]},
            }
        }
        try:
            users = db.delete(self.TableName, **kwargs)
        except ClientError as e:
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        return deserialize(users), 204


class DeleteItemCondition(Resource):
    ''' DeleteItemCondition '''
    TableName = "users"

    def delete(self):
        body = request.get_json()
        kwargs = {
            "Key": {
                "first_name": {"S": body["first_name"]},
                "email": {"S": body["email"]},
            },
            "ConditionExpression": "age < :a",
            "ExpressionAttributeValues": {
                ":a": {"N": str(body["age"])},
            }
        }
        try:
            users = db.delete(self.TableName, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return Response(
                    response=json.dumps({"message": "Invalid input for operation"}), 
                    status=400, mimetype='application/json')
            return Response(
                response=json.dumps({"error": str(e)}), 
                status=500, mimetype='application/json')
        return deserialize(users), 204

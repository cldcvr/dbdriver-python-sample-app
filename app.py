import os
import logging
import sys

from flask import Flask
from flask_restful import Api
from ecommercy.routes.users import User, QueryUsers, QueryUserID,\
    QueryUserBegins, QueryIndexAll, QueryIndexRange, UpdateOrPutItemID, \
        UpdateItemIfExists, UpdateItemCondition, DeleteItem, DeleteItemCondition
from ecommercy.routes.batch import BatchWrite, BatchGet

# This interface needs to be added to the application,
# Responsible in switching the boto library to point to either AWS or
# the Dynamodb Spanner adapter
# from ecommercy.db.db import db

os.environ["DYNAMO_LOCAL_HOST"] = "localhost"
os.environ["DYNAMO_LOCAL_PORT"] = "9050"

app = Flask(__name__)
api = Api(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


logging.info("Register routes")


api.add_resource(User, "/user")
api.add_resource(QueryUsers, "/query/user")
api.add_resource(QueryUserID, "/query/key/user")
api.add_resource(QueryUserBegins, "/query/begins/user")
api.add_resource(QueryIndexAll, "/query/index/users")
api.add_resource(QueryIndexRange, "/query/index/range")
api.add_resource(UpdateOrPutItemID, "/update/user")
api.add_resource(UpdateItemIfExists, "/update/exists/user")
api.add_resource(UpdateItemCondition, "/update/condition/user")
api.add_resource(DeleteItem, "/delete/user")
api.add_resource(DeleteItemCondition, "/delete/condition/user")
api.add_resource(BatchWrite, "/batch/write")
api.add_resource(BatchGet, "/batch/get")

if __name__ == "__main__":
    app.run(port=5050, debug=True, threaded=True)

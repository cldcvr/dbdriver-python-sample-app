  
DynamoDB/Spanner sample RESTful API

=========================

A demo app to showcase the `dynamodb` adapter

  
## URL mappings

*  **GET /user** - get user by partition key and sort key
*  **POST /query/user** - query user by just partition key
*  **POST /query/key/user** - query user by partition and sort key
*  **POST /query/begins/user** - query users by partition key and where sort key begins with (prefix)
*  **POST /query/index/users** - query users on partition key of secondary Index
*  **POST /query/index/range** - query users on partition and sort key of secondary index, where sort key ranges between values
*  **POST /update/user** - update user or put if not exists
*  **POST /update/exists/user** - update user only if partition key exists
*  **POST /update/condition/user** - update user on condition where value is greater than existing
*  **POST /delete/user** - delete user
*  **POST /batch/get** - batch get users
  

## User model

*  **first_name** (_partitionKey_): **string**
*  **last_name** : **string**
*  **email** (_sortkey_): **string**
*  **country** (_partitionKeyOfSecondaryIndex_): **string**
*  **price** (_sortkeyOfSecondaryIndex_): **number**


## Usage

The application can be executed to use `dynamodb` or `spanner` for  CRUD operations


### 1. Run application with Dynamodb as the data source

The application uses `ap-southeast-1` as the default region.

To connect to any other region, run

```
export AWS_REGION="region"
```


### 2. Run application with Spanner as the data source

We need to uncomment lines 17 & 18, in `app.py`

```py
os.environ["DYNAMO_LOCAL_HOST"] = "dynamodb-adapter-ip"; eg ('localhost')

os.environ["DYNAMO_LOCAL_PORT"] = "9050"
```

To run application, call:

```
pip install -r requirements.txt
python app.py
```
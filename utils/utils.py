
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

deserializer = TypeDeserializer()
serializer = TypeSerializer()

def deserialize(item):
    deserialised = {}
    for k, v in item.items():
        val = deserializer.deserialize(v)
        if isinstance(val, Decimal):
            val = str(val)
        deserialised[k] = val
    return deserialised

def serialize(item):
    #import pdb; pdb.set_trace()
    serialized = {}
    for k, v in item.items():
        if v != "":
            val = serializer.serialize(v)
            serialized[k] = val
    return serialized
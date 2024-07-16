# mongodb.py
import motor.motor_asyncio
from bson.objectid import ObjectId

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
db = client.csvmerger

async def create_request(file_ids):
    document = {'file_ids': file_ids, 'status': 'processing', 'result_file_id': None}
    result = await db.requests.insert_one(document)
    return str(result.inserted_id)

async def update_request(request_id, result_file_id):
    await db.requests.update_one({'_id': ObjectId(request_id)}, {'$set': {'status': 'completed', 'result_file_id': result_file_id}})

async def get_request(request_id):
    document = await db.requests.find_one({'_id': ObjectId(request_id)})
    if document:
        document['_id'] = str(document['_id'])
    return document

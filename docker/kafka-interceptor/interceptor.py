import json, os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pymongo import MongoClient, ASCENDING
from pymongo.synchronous.collection import Collection
from functools import lru_cache
from typing import Optional
from confluent_kafka import Producer

producer = Producer({ 'bootstrap.servers': 'kafka:9092' })

class HTTPHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        """Set CORS headers for both preflight and actual responses"""
        self.send_header('Access-Control-Allow-Origin', 'http://localhost:5000')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        """Handle preflight OPTIONS request"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == '/ping':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'pong')

    def do_POST(self):
        """Handle different POST requests"""

        # Handle requests from fictional API's
        if self.path == '/topics':
            data = self.rfile.read(int(self.headers.get('Content-Length')))
            topic = 'persons'

            try:
                producer.produce(topic, value=data)
                producer.flush()
                print(f"Received and sent to Kafka topic: {topic}")
            except Exception as e:
                print(f"Kafka error: {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'Kafka producer error')
                return

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'ok'}).encode())

        # Handle requests from browser
        if self.path == '/persons':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)

            collection = get_collection('pymongo', 'pymongo', 'data', 'persons', check_credentials=True)
            data = fetch_from_mongo(post_data, collection)
            response_body = json.dumps(data).encode('utf-8')

            self.send_response(200)
            self._send_cors_headers()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

@lru_cache(1)
def get_collection(
        user: str,
        password: str,
        database: str,
        collection: str,
        ip: str = 'mongodb',
        port: int = 27017,
        check_credentials: bool = False
    ):
    """
    Login in MongoDB collection

    Parameters
    ----------
        user : str :
            Username for MongoDB
        password : str:
            Password for MongoDB
        database : str :
            Database in which login will happen
        collection : str :
            Collection in which login will happen
        ip : str :
            IP where MongoDB reside
        port : int :
            Port where MongoDB reside
        check_credential : bool :
            Need to validate credentials, database and etc. or not
    Return
    ------
    Collection :
        Collection MongoDB object
    """
    uri = f"mongodb://{user}:{password}@{ip}:{port}/{database}?authSource={database}"
    client = MongoClient(uri)
    if check_credentials:
        client.admin.command('ping')
    db = client[database]
    return db[collection]

def pagination(
        collection: Collection,
        filter_column: str = 'user_id',
        last_row: Optional[str] = None,
        last_id: Optional[str] = None,
        unique_filter: Optional[str] = 'user_id',
        page_size: int = 20
        ) -> list[dict]:
    """
    Fetcthing pages for pagination

    Parameters
    ----------
    collection : Collection :
        MongoDB collection object
    filter_column : str :
        Field by which filtering and sorting will happen
    last_row : Optional[str] :
        Last element that consumer saw of filter_column
    last_id : Optional[str] :
        ID associated with last_row
    unique_filter_column : str :
        Unique column by which filtering will happen if 'filter_column' is not provided
    page_size : int :
        Number of documents that need to be returned

    Return
    ------
    list[dict] :
        List of documents
    """
    if last_id is None:
        cursor = collection.find({})
        cursor = cursor.sort([
            (filter_column, ASCENDING)
        ]).limit(page_size)
    else:
        if filter_column == unique_filter:
            filter_condition = { filter_column: {"$gt": last_id} }
        else:
            filter_condition = {
                "$or": [
                    {filter_column: {"$gt": last_row}},
                    {filter_column: last_row, unique_filter: {"$gt": last_id}}
                ]
            }
        cursor = collection.find(filter_condition)

        cursor = cursor.sort([
            (filter_column, ASCENDING),
            (unique_filter, ASCENDING)
        ]).limit(page_size)

    return list(cursor)

def init_data():
    '''Create initial data for kafka initialization'''
    init_data = {
        'name': 'init',
        'country_code': 'IN',
        'city': 'Init',
        'street_name': 'Init',
        'postcode': 1,
        'email': 'init@init.com',
        'ipv4': '100.000.00.00',
        'ipv6': 'a00a:00a0:0000:000:0aa0:0aaa:a00a:0a0a',
        'user_id': 1
    }
    producer.produce('persons', value=json.dumps(init_data))
    producer.flush()

def remove_id_column(documents: list[dict]):
    '''Remove internal not necessary MongoDB column'''
    for document in documents:
        del document[next(iter(document))]
    return documents

def fetch_from_mongo(byte: bytes, collection: Collection):
    data = json.loads(byte)
    print(data, flush=True)

    # If == 0 mean user just entered the page, intial data will be pulled
    if len(data.items()) == 0:
        return remove_id_column(pagination(collection))

    # If == 1 (only 1 item came from user) and its metadata column,
    # mean user doean't applied any filter, will just fetch next data
    if len(data.items()) == 1 and next(iter(data)) == 'metadata':
        return remove_id_column(pagination(collection, last_id=data['metadata']))

    # If == 1 and its not metadata mean user want to filter data by some other column
    # will fetch result sorted by specified column
    elif len(data.items()) == 1 and next(iter(data)) != 'metadata':
        return remove_id_column(pagination(collection, filter_column=next(iter(data))))

    # Else user have filter applied and it isn't his first fetch, will fetch
    # next page of users and sort by column which user have currenly active
    else:
        i = False
        for key, value in data.items():
            if i:
                last_id = value
            else:
                filter_column, last_row = key, value
                i += True
        return remove_id_column(pagination(collection, filter_column, last_row, last_id))

if __name__ == '__main__':
    url = { "domain": "0.0.0.0", "port": 5000 }
    server = HTTPServer((url['domain'], url['port']), HTTPHandler)
    init_data()
    print(f"HTTP Interceptor running on http://{url['domain']}:{url['port']}")
    server.serve_forever()

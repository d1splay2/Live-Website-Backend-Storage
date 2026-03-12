import json, os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pymongo import MongoClient, ASCENDING
from pymongo.synchronous.collection import Collection
from functools import lru_cache
from typing import Optional
from confluent_kafka import Producer


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

            collection = get_collection('pymongo', 'pymongo', 'test', 'raw_persons', check_credentials=True)
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
        ip = 'localhost',
        port = 27017,
        check_credentials: bool = False
    ):
    """
    Login in MongoDB collection

    Args:
        user: Username for MongoDB
        password: Password for MongoDB
        database: Database in which login will happen
        collection: Collection in which login will happen
        ip: IP where MongoDB reside
        port: Port where MongoDB reside
        check_credential: Need to validate credentials, database and etc. or not
    Returns:
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
        page_size: int = 20
        ) -> list[dict]:
    """
    Fetcthing pages for pagination

    Args:
        collection: MongoDB collection object
        filter_column: Field by which filtering and sorting will happen
        last_row: Last element that consumer saw of filter_column
        last_id: ID associated with last_row
        page_size: Number of documents that need to be returned
    Returns:
        List of documents
    """
    if last_row is None or last_id is None:
        cursor = collection.find({})
    else:
        filter_condition = {
            "$or": [
                {filter_column: {"$gt": last_row}},
                {filter_column: last_row, "user_id": {"$gt": last_id}}
            ]
        }
        cursor = collection.find(filter_condition)

    cursor = cursor.sort([
        (filter_column, ASCENDING),
        ("user_id", ASCENDING)
    ]).limit(page_size)

    return list(cursor)

def remove_id_column(documents: list[dict]):
    for document in documents:
        del document[next(iter(document))]
    return documents

def fetch_from_mongo(byte: bytes, collection: Collection):
    data = json.loads(byte)

    # If no data provided will sort by default filtering column without pagination
    if len(data.items()) == 0:
        return remove_id_column(pagination(collection))

    # If only filtering column provided then sort by this column without pagination
    if len(data.items()) == 1:
        return remove_id_column(pagination(collection, next(iter(data))))

    # If all necessary values provided will apply pagination
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
    producer = Producer({ "bootstrap.servers": 'localhost:9092' })
    url = { "domain": "localhost", "port": 5050 }
    server = HTTPServer((url['domain'], url['port']), HTTPHandler)
    print(f"HTTP Interceptor running on http://{url['domain']}:{url['port']}")
    server.serve_forever()

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": 'localhost:9092',
}

url = {
    "domain": "localhost",
    "port": 5000
}

producer = Producer(producer_config)

class KafkaHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
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

if __name__ == '__main__':
    server = HTTPServer((url['domain'], url['port']), KafkaHTTPHandler)
    print(f"HTTP Interceptor running on http://{url['domain']}:{url['port']}")
    server.serve_forever()

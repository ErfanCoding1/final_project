from flask import Flask, request
import requests
from kafka import KafkaProducer
import json
import redis

class IngestApp:
    def __init__(self, name, kafka_server, kafka_topic, redis_host='localhost', redis_port=6379):
        self.app = Flask(name)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic
        self.valid_stocks = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

        # Initialize Redis client
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.add_routes()

    def add_routes(self):
        self.app.add_url_rule('/ingest', 'ingest', self.ingest, methods=['POST'])
        self.app.add_url_rule('/', 'home', self.home, methods=['GET'])
        self.app.add_url_rule('/cache', 'cache', self.cache_data, methods=['POST'])

    def validate_data(self, data):
        if 'timestamp' not in data:
            return 'Invalid data: missing timestamp', 400
        if 'stock_symbol' not in data or data['stock_symbol'] not in self.valid_stocks:
            return 'Invalid data: missing or invalid stock_symbol', 400
        if 'data_type' in data:
            if data['data_type'] == 'order_book':
                required_fields = ['order_type', 'price', 'quantity']
            elif data['data_type'] == 'news_sentiment':
                required_fields = ['sentiment_score', 'sentiment_magnitude']
            elif data['data_type'] == 'market_data':
                required_fields = ['market_cap', 'pe_ratio']
            elif data['data_type'] == 'economic_indicator':
                required_fields = ['indicator_name', 'value']
            else:
                return 'Invalid data: unknown data_type', 400

            for field in required_fields:
                if field not in data:
                    return f'Invalid data: missing {field} for {data["data_type"]}', 400
        else:
            required_fields = ['opening_price', 'closing_price', 'high', 'low', 'volume']
            for field in required_fields:
                if field not in data:
                    return f'Invalid data: missing {field}', 400
        return None

    def ingest(self):
        data = request.get_json()
        validation_error = self.validate_data(data)
        if validation_error:
            return validation_error
        print(data)
        self.producer.send(self.kafka_topic, data)
        return 'Data received!', 200

    def cache_data(self):
        data = request.get_json()
        if not data or 'key' not in data or 'value' not in data:
            return 'Invalid data: missing key or value', 400
        key = data['key']
        value = data['value']

        # Store data in Redis
        self.redis_client.set(key, value)
        return f'Data cached with key: {key}', 200

    def home(self):
        api_endpoint = "http://localhost:5000/ingest"
        try:
            response = requests.get(api_endpoint)
            print(f"Data from endpoint: {response.json()}")
            return f"Data from endpoint: {response.json()}"
        except requests.RequestException as e:
            return f"Error fetching data: {str(e)}", 500

    def run(self, port=5000):
        self.app.run(port=port)

if __name__ == "__main__":
	app = IngestApp(name="IngestApp", kafka_server='localhost:9092', kafka_topic='dataTopic')
	app.run(port=5000)

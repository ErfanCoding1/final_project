from flask import Flask, request
from kafka import KafkaProducer
import json
import datetime
import redis

class DataIngestionService:
    def __init__(self, kafka_broker='localhost:9092', topic='financial-data-topic', redis_host='localhost', redis_port=6379):
        self.app = Flask(__name__)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic

        # Connect to Redis
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

        self.setup_routes()
    
    def setup_routes(self):
        @self.app.route('/ingest', methods=['POST'])
        def ingest():
            data = request.get_json()
            
            # Check for duplicate data in Redis
            if self.is_duplicate(data):
                return 'Duplicate data: already processed', 400
            
            # Validate the incoming data
            validation_result = self.validate_data(data)
            if validation_result != "valid":
                return validation_result, 400
            
            # Send data to Kafka
            self.producer.send(self.topic, data)
            
            # Store data in Redis to prevent duplicates
            self.store_in_redis(data)
            return 'Data received and sent to Kafka!', 200

        @self.app.route('/')
        def home():
            return 'Welcome to the Data Ingestion Service!', 200
    
    def validate_data(self, data):
        # Check required fields
        required_fields = ['timestamp', 'stock_symbol', 'data_type']
        for field in required_fields:
            if field not in data:
                return f'Invalid data: missing {field}'
        
        # Validate timestamp format
        try:
            datetime.datetime.fromisoformat(data['timestamp'])
        except ValueError:
            return 'Invalid data: timestamp must be in ISO 8601 format'

        # Validate stock symbol
        valid_stocks = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]
        if data['stock_symbol'] not in valid_stocks:
            return 'Invalid data: invalid stock_symbol'

        # Validate numeric fields
        numeric_fields = ['opening_price', 'closing_price', 'high', 'low', 'volume']
        for field in numeric_fields:
            if field in data and data[field] < 0:
                return f'Invalid data: {field} must be a positive number'

        # Validate specific data types
        if 'data_type' in data:
            if data['data_type'] == 'order_book':
                required_order_fields = ['order_type', 'price', 'quantity']
                for field in required_order_fields:
                    if field not in data:
                        return f'Invalid data: missing {field} for order_book'
            elif data['data_type'] == 'news_sentiment':
                required_sentiment_fields = ['sentiment_score', 'sentiment_magnitude']
                for field in required_sentiment_fields:
                    if field not in data:
                        return f'Invalid data: missing {field} for news_sentiment'
            elif data['data_type'] == 'market_data':
                required_market_fields = ['market_cap', 'pe_ratio']
                for field in required_market_fields:
                    if field not in data:
                        return f'Invalid data: missing {field} for market_data'
            elif data['data_type'] == 'economic_indicator':
                required_indicator_fields = ['indicator_name', 'value']
                for field in required_indicator_fields:
                    if field not in data:
                        return f'Invalid data: missing {field} for economic_indicator'
            else:
                return 'Invalid data: unknown data_type'
        
        return "valid"

    def is_duplicate(self, data):
        """Check if the data has already been processed."""
        data_id = self.generate_data_id(data)
        return self.redis_client.exists(data_id)

    def store_in_redis(self, data):
        """Store the data in Redis with an expiration time."""
        data_id = self.generate_data_id(data)
        self.redis_client.set(data_id, "processed", ex=3600)  # Expires in 1 hour

    def generate_data_id(self, data):
        """Generate a unique identifier for the data."""
        return f"{data['timestamp']}_{data['stock_symbol']}_{data['data_type']}"

    def run(self, port=5000):
        self.app.run(port=port)

# Instantiate and run the service
if __name__ == "__main__":
    service = DataIngestionService()
    service.run()

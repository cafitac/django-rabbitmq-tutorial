import json
from os import environ

import django
import pika
from django.conf import settings

environ.setdefault("DJANGO_SETTINGS_MODULE", 'django_project.settings')
django.setup()


class Producer:
    """RabbitMQ message sender"""

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=settings.RABBITMQ_HOST,
                heartbeat=settings.RABBITMQ_HEARTBEAT,
                blocked_connection_timeout=settings.RABBITMQ_CONNECTION_TIMEOUT,
            )
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=settings.RABBITMQ_QUEUE_NAME,
        )
        print("Producer connected")

    def publish(self, exchange: str = "", routing_key: str = settings.RABBITMQ_QUEUE_NAME, body: dict = None) -> None:
        if body is None:
            body = {}

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(body),
        )
        print("Send message")

    def __del__(self):
        print("Producer connection closed")
        self.connection.close()


if __name__ == "__main__":
    Producer().publish(body={"message": "test"})

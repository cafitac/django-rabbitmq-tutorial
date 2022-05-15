import threading

import pika
from django.conf import settings
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic


class Consumer(threading.Thread):
    """Rabbitmq message receiver"""

    def __init__(self):
        super().__init__()
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
        print("Consumer connected")

    def start_consuming(self):
        self.channel.basic_consume(
            queue=settings.RABBITMQ_QUEUE_NAME,
            on_message_callback=self.receive_callback,
            auto_ack=True,
        )
        print("Start consuming")
        self.channel.start_consuming()

    def receive_callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        print(" [x] Received %r" % body)

    def run(self) -> None:
        self.start_consuming()

    def __del__(self):
        print("Consumer connection closed")
        self.connection.close()

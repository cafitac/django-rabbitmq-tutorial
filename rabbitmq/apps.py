from django.apps import AppConfig

from rabbitmq.consumer import Consumer


class RabbitmqConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "rabbitmq"

    def ready(self):
        consumer = Consumer()
        consumer.daemon = True
        consumer.start()

import os
import pika
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.m_routing_key = routing_key
        self.m_exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()
    

    def setupRMQConnection(self) -> None:
        # pika is a python client for RabbitMQ.

        # Parse the url to extract the params necessary to connect to RabbitMQ
        connection_params = pika.URLParameters(os.environ["AMQP_URL"]) 

        # Connect to RabbitMQ with the extracted params
        # "Blocking" means that operations on this connection will block/wait until they are completed
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html
        self.m_connection = pika.BlockingConnection(parameters=connection_params)

        # A channel object is used to communicate with RabbitMQ via the AMQP RPC methods
        # https://pika.readthedocs.io/en/stable/modules/channel.html
        self.m_channel = self.m_connection.channel()

        # Ensure that the specified queue exists - if it doesn’t, create it.
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html?highlight=queue_declare#pika.adapters.blocking_connection.BlockingChannel.queue_declare
        self.m_channel.queue_declare(queue=self.m_queue_name)

        # Ensure that the specified exchange exists - if it doesn’t, create it
        # "Exchange" is a routing mechanism that receives messages from producers + determines which queues they should be delivered to
        # Exchanges don't store messages - they route messages based on certain rules or bindings
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html?highlight=exchange_declare#pika.adapters.blocking_connection.BlockingChannel.exchange_declare
        self.m_channel.exchange_declare(self.m_exchange_name)
    
    def publishOrder() -> None:
        pass

    def __del__() -> None:
        pass


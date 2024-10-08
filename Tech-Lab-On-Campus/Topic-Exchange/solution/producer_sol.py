import os
import pika
from producer_interface import mqProducerInterface


# Consumer Service File

# HOW TO RUN TEST

# 1) Enter bash shell
    # If you haven't started the container
        # docker-compose up -d && docker-compose exec rmq_lab /bin/bash
    # If you have started the container (where the string is from your other bash shell e.g. jovyan@a492aec1d9b)
        # docker exec -it a492aec1d9bb /bin/bash 

# 2) Run python script
    # If on part 1: 
        # cd Producer-And-Consumer/consumer/
        # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 consume.py
    # If on part 2:
        # cd Topic-Exchange
        # [CONSUMER] Subscribe to tech sector
            # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 consume.py -s tech -q techLabQueue
        # [CONSUMER] Subscribe to health sector
            # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 consume.py -s health -q techLabQueue
        # [PRODUCER] Send three messages: first, a tech stock; second, health; and third, a different sector.
            # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 publish.py -t NVDA -p 130 -s tech
                # Should output [x] Received Message: b'NVDA is now $130' in tech consumer 
            # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 publish.py -t JNJ -p 166 -s health
                # Should output [x] Received Message: b'JNJ is now $166' in health consumer 
            # AMPQ_URL="amqp://guest:guest@rabbitmq:5672/" python3 publish.py -t XOM -p 114 -s energy
                # Should have no output since we did not set up a consumer for the energy sector. 


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

        # Ensure that the specified exchange exists - if it doesn’t, create it
        # "Exchange" is a routing mechanism that receives messages from producers + determines which queues they should be delivered to
        # Exchanges don't store messages - they route messages based on certain rules or bindings
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html?highlight=exchange_declare#pika.adapters.blocking_connection.BlockingChannel.exchange_declare
        self.m_channel.exchange_declare(self.m_exchange_name, exchange_type="topic")
    
    def publishOrder(self, message: str) -> None:
        # Publish the message to the exchange
        # https://pika.readthedocs.io/en/stable/modules/channel.html?highlight=basic_publish#pika.channel.Channel.basic_publish
        self.m_channel.basic_publish(
            exchange = self.m_exchange_name,
            routing_key = self.m_routing_key,
            body = message,
        )

        print(" [x] Sent Orders")

    def __del__(self) -> None:
        print(f"Closing RMQ connection on destruction")
        self.m_channel.close()
        self.m_connection.close()


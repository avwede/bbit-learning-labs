import os
import pika
from consumer_interface import mqConsumerInterface

# mqConsumer inherits from mqConsumerInterface
class mqConsumer(mqConsumerInterface):
    # Constructor - called when the class is first created
    # -> None indicates the return type
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Take in parameters and save it to self
        # "Self" references the instance of the class itself
        # Each parameter is defined in mqConsumerInterface
        self.m_binding_key = binding_key 
        self.m_queue_name = queue_name
        self.m_exchange_name = exchange_name

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # pika is a python client for RabbitMQ.

        # Get the RabbitMQ connection URL from the environment var "AMPQ_URL"
        # Parse the url to extract the params necessary to connect to RabbitMQ
        connection_params = pika.URLParameters(os.environ["AMPQ_URL"])

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

        # Bind the queue and exchange
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html?highlight=queue_bind#pika.adapters.blocking_connection.BlockingChannel.queue_bind
        self.m_channel.queue_bind(
            queue=self.m_queue_name,
            routing_key=self.m_binding_key,
            exchange=self.m_exchange_name,
        )

        # Set up consumer via basic_consume
        # Set up callback function for receiving messages via on_message_callback (when the message is received, this callback is invoked)
        # auto_ack=False - messages aren't automatically acknowledged by RabbitMQ once delivered to the consumer
        self.m_channel.basic_consume(
            self.m_queue_name, self.on_message_callback, auto_ack=False
        )

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Manually acknowledge message
        # method_frame: contains metadata about the message delivery (e.g. delivery tag)
        # header_frame: contains metadata about message headers (e.g. content type)
        # The second parameter (False) is for multiple
            # False - only the specific message with this delivery_tag is acknowledged
            # True - all messages up to/including the delivery_tag are acknowledged
        channel.basic_ack(method_frame.delivery_tag, False)

        # Print message
        print(f" [x] Received Message: {body}")

    def startConsuming(self) -> None:
        # Print log
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        # https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html?highlight=start_consuming#pika.adapters.blocking_connection.BlockingChannel.start_consuming
        self.m_channel.start_consuming()

    def __del__(self) -> None:
        print(f"Closing RMQ connection on destruction")
        self.m_channel.close()
        self.m_connection.close()
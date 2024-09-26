# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys

from solution.producer_sol import mqProducer  # pylint: disable=import-error

# Producer Service File

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


def main(ticker: str, price: float, sector: str) -> None:
    # Implement Logic to Create Routing Key from the ticker and sector variable -  Step 2
    routingKey = f"Stock.{ticker}.{sector}"

    # Remove whitespace
    routingKey.strip()

    # Create an intance of the mqProducer class
    producer = mqProducer(routing_key = routingKey, exchange_name = "Tech Lab Topic Exchange")

    # Implement Logic To Create a message variable from the variable EG. "TSLA price is now $500" - Step 3
    message = f"{ticker} is now ${price}"
    
    # Publish Message
    producer.publishOrder(message)

if __name__ == "__main__":

    # Implement Logic to read the ticker, price and sector string from the command line and save them - Step 1
    # Create an instance of ArgumentParser 
    # The description is what will be displayed when you use the --help flag
    # argparse - https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        description="Process Stock Ticker, Price And Type."
    )

    # Add command line arguments
    # Expected values of arguments are all strings
    # All arguments are mandatory
    parser.add_argument(
        "-t", "--ticker", type=str, help="Stock Ticker", required=True
    )
    parser.add_argument(
        "-p", "--price", type=str, help="Stock Price", required=True
    )
    parser.add_argument(
        "-s", "--sector", type=str, help="Stock Sector", required=True
    )

    # Parse arguments and assign them to `args`
    # The arguments are now accessible on the args object via args.sector or args.queue
    args = parser.parse_args()

    sys.exit(main(args.ticker, args.price, args.sector))

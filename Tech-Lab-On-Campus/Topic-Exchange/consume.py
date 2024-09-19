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

from solution.consumer_sol import mqConsumer  # pylint: disable=import-error

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

def main(sector: str, queueName: str) -> None:
    
    # Implement Logic to Create Binding Key from the ticker and sector variable -  Step 2
   
    # When binding the Queue to the exchange ensure that routing key utilizies one of the special cases
    # for binding keys such as * (star) or # (hash). For instance. "#.Google.#" as a binding key for a 
    # Queue will ensure that all messages with a key containing the word Google such as Stock.Google.tech 
    # will have their messages routed to that Queue.

    # star (*) will match exactly one word
    # hash (#) will match zero of more words
    # The f-string format (f"") allows for embedding the value of sector directly into the string
    # bindingKey example matches could be "test.test.Tech" if the sector is "Tech"
    bindingKey = f"*.*.{sector}"

    # Remove whitespace
    bindingKey.strip()

    # Create an intance of the mqConsumer class
    consumer = mqConsumer(
        binding_key = bindingKey,
        exchange_name = "Tech Lab Topic Exchange",
        queue_name = queueName
    )    
    
    # Start it up!
    consumer.startConsuming()

if __name__ == "__main__":

    # Implement Logic to read the sector and queueName string from the command line and save them - Step 1
    
    # Create an instance of ArgumentParser 
    # The description is what will be displayed when you use the --help flag
    # argparse - https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        description="Process Stock Sector, Add Queue Name"
    )

    # Add first command line argument: -s / --sector
    # Expected value of argument is a string
    # Argument is mandatory
    parser.add_argument(
        "-s",
        "--sector",
        type=str,
        help="Stock Sectors",
        required=True,
    )

    # Add second command line argument: -q / --queue
    # Expected value of argument is a string
    # Argument is mandatory
    parser.add_argument(
        "-q",
        "--queue",
        type=str,
        help="Queue Name",
        required=True,
    )

    # Parse arguments and assign them to `args`
    # The arguments are now accessible on the args object via args.sector or args.queue
    args = parser.parse_args()

    sys.exit(main(args.sector, args.queue))

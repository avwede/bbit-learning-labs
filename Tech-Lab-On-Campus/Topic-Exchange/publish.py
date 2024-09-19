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

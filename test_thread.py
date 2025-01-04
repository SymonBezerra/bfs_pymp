import argparse
import logging

args = argparse.ArgumentParser()
args.add_argument('--port', type=int, default=5001)
args = args.parse_args()

from thread import Thread

client = Thread('0.0.0.0', args.port)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
LOGGER.addHandler(console_handler)

if __name__ == '__main__':
    while True:
        data = client.recv()
        LOGGER.info(f'Received {data}')

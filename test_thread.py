import argparse
from collections import deque
import logging

from thread import Thread
from message import Message

args = argparse.ArgumentParser()
args.add_argument('--port', type=int, default=5001)
args.add_argument('--server_ip', type=str, default='0.0.0.0')
args.add_argument('--server_port', type=int, default=5000)
args = args.parse_args()

client = Thread('0.0.0.0', args.port, args.server_ip, args.server_port)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
LOGGER.addHandler(console_handler)

if __name__ == '__main__':
    while True:
        data = client.socket.recv()
        LOGGER.info(data)
        client.exec(data)
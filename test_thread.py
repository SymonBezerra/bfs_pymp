import argparse
from collections import deque
import errno
import logging
import threading
import select
import socket
import time

from thread import Thread

args = argparse.ArgumentParser()
args.add_argument('--receive', type=int, default=5001)
args.add_argument('--confirm', type=int, default=5002)
args = args.parse_args()


client = Thread('0.0.0.0', args.receive, args.confirm)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
LOGGER.addHandler(console_handler)

if __name__ == '__main__':
    message_queue = deque()
    lock = threading.Lock()
    def recv():
        while True:
            lock.acquire()
            msg, addr = client.recv()
            if msg and addr: 
                message_queue.append((msg, addr))
                LOGGER.info(f'Added message to queue: {msg.build()}')
            lock.release()
    
    def exec():
        while True:
            lock.acquire()
            if message_queue:
                msg, addr = message_queue.popleft()
                client.exec(msg, addr)
            lock.release()

    threading.Thread(target=recv).start()
    threading.Thread(target=exec).start()

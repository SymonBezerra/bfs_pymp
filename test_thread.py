import argparse
from collections import deque
import logging
import zmq

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
    # Initialize poller
    poller = zmq.Poller()
    
    # Register both sockets with the poller
    poller.register(client.socket, zmq.POLLIN)
    poller.register(client.pull_socket, zmq.POLLIN)
    
    while True:
        try:
            # Poll sockets with a timeout (e.g., 1000ms)
            sockets = dict(poller.poll(1000))
            
            # Check client.socket
            if client.socket in sockets:
                data = client.socket.recv()
                LOGGER.info(f"Received from socket: {data}")
                client.exec(data)
            
            # Check client.pull_socket
            if client.pull_socket in sockets:
                data = client.pull_socket.recv()
                LOGGER.info(f"Received from pull socket: {data}")
                client.exec(data)
                
        except KeyboardInterrupt:
            # Clean shutdown
            poller.unregister(client.socket)
            poller.unregister(client.pull_socket)
            client.socket.close()
            client.pull_socket.close()
            break
        except Exception as e:
            LOGGER.error(f"Error in main loop: {e}")
            raise e
import argparse
from collections import deque
import logging
import threading
import select
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
    new_message_event = threading.Event()
    running = True

    def recv():
        while running:
            try:
                # Check if socket is ready to read
                ready = select.select([client.receiving_socket], [], [], 0.1)
                if ready[0]:
                    msg, addr = client.recv()
                    if msg and addr:
                        with lock:
                            message_queue.append((msg, addr))
                            LOGGER.info(f'Added message to queue: {msg.build()}')
                        new_message_event.set()  # Signal new message
                else:
                    time.sleep(0.01)  # Small sleep if no data
            except Exception as e:
                LOGGER.error(f"Error in receiver: {e}")
                time.sleep(0.1)  # Sleep on error
    
    def exec():
        while running:
            # Wait for new messages
            new_message_event.wait(timeout=0.1)
            
            try:
                with lock:
                    if message_queue:
                        msg, addr = message_queue.popleft()
                        if not message_queue:  # Clear event if queue empty
                            new_message_event.clear()
                    else:
                        continue  # Skip if no messages
                
                # Process message outside lock
                client.exec(msg, addr)
                
            except Exception as e:
                LOGGER.error(f"Error in executor: {e}")
                time.sleep(0.1)
                raise e

    # Start threads
    recv_thread = threading.Thread(target=recv)
    exec_thread = threading.Thread(target=exec)
    
    recv_thread.daemon = True
    exec_thread.daemon = True
    
    recv_thread.start()
    exec_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        running = False
        new_message_event.set()  # Wake up executor thread
        recv_thread.join(timeout=1)
        exec_thread.join(timeout=1)
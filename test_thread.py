import argparse

args = argparse.ArgumentParser()
args.add_argument('--port', type=int, default=5001)
args = args.parse_args()

from thread import Thread

client = Thread('0.0.0.0', args.port)

if __name__ == '__main__':
    while True:
        data = client.recv()
        # print(data)

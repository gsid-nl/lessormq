"""Publish three messages and work them off with the LessorMQ worker.

LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); `pip install lessormq`.
"""
import os
import sys

from lessormq import Client, Job, Message, Worker

client = Client(os.environ.get("LESSORMQ_URL", "http://127.0.0.1:18080"), os.environ["LESSORMQ_KEY"])
queue = os.environ.get("LESSORMQ_QUEUE", "examples")

for body in ({"n": 1}, {"n": 2, "fail_once": True}, {"n": 3}):
    print("published", client.publish(queue, body).id)

handled = 0


def handle(message: Message, job: Job) -> None:
    global handled
    n = message.body["n"]
    if message.body.get("fail_once") and message.attempt == 1:
        print(f"n={n} attempt={message.attempt} failed")
        raise RuntimeError("temporarily unavailable")

    print(f"n={n} attempt={message.attempt} handled")
    handled += 1
    if handled == 3:
        worker.stop()


worker = Worker(client, queue, handle, consumer="python-example", wait_seconds=5, max_seconds=60)
worker.run()
print(f"done: {handled} handled")
sys.exit(0 if handled == 3 else 1)

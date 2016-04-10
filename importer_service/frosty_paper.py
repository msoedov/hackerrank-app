import sys
import logging
import asyncio
from asyncio import Queue, Event
import asynqp
from retry import retry

log = logging.getLogger()
log.setLevel(logging.DEBUG)
sys.setcheckinterval(500)


@retry((ConnectionRefusedError,), tries=10, delay=1)
async def new_ampq_sender():
    # connect to the RabbitMQ broker
    connection = await asynqp.connect('docker', 5672, username='yo', password='yo')

    # Open a communications channel
    channel = await connection.open_channel()

    # Create a queue and an exchange on the broker
    exchange = await channel.declare_exchange('email.exchange', 'direct')
    queue = await channel.declare_queue('email.queue')

    # Bind the queue to the exchange, so the queue will get messages published to the exchange
    await queue.bind(exchange, 'routing.key')

    def sender(payload):
        msg = asynqp.Message(payload)
        exchange.publish(msg, 'routing.key')

    async def finalizer():
        await channel.close()
        await connection.close()
    return sender, finalizer


async def spredsheet_reader(queue, done):
    part = 0
    with open('data.csv', 'rb') as f:
        while True:
            line = f.readline()  # blocking call but not a big deal, huh?
            if not line:
                done.set()
                return
            part += 1
            try:
                full_name, email = line.decode('utf-8').strip('\n').split(',')
            except ValueError:
                pass
            else:
                while True:
                    try:
                        await queue.put({'fullname': full_name, 'email': email, 'line': part})
                        break
                    except asyncio.QueueFull:
                        print('Blocked')
                        await asyncio.sleep(.2)
                        continue


async def rabbit_sender(queue, rabbit_sender):
    while True:
        msg = await queue.get()
        print('Sent {}'.format(msg))
        rabbit_sender(msg)
        queue.task_done()


async def spawn(loop):
    sender, finalizer = await new_ampq_sender()
    entries_queue = Queue(40)
    tasks = []
    done = Event()
    t = loop.create_task(spredsheet_reader(entries_queue, done))
    tasks.append(t)
    for _ in range(40):
        t = loop.create_task(rabbit_sender(entries_queue, sender))
        tasks.append(t)
    await done.wait()
    await entries_queue.join()
    # cancel rabbit_sender's
    [t.cancel() for t in tasks]
    await finalizer()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(spawn(loop))

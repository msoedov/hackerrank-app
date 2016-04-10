import logging
import os

import aiopg
import asyncio
import asynqp
import psycopg2


schema = """CREATE TABLE IF NOT EXISTS users (
    email VARCHAR(100) NOT NULL,
    fullname VARCHAR(100),
    times_received INTEGER NOT NULL
);
--- CREATE UNIQUE INDEX users_email_uindex ON users (email);
"""

update_query = """
--- Cool kids use ON CONFLICT
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN TRANSACTION;
UPDATE users set times_received = times_received + 1 where email = %(email)s;
INSERT INTO users (email, fullname, times_received) SELECT %(email)s, %(fullname)s, 1
WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = %(email)s);
commit TRANSACTION;
"""


log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())


async def retry_fut(fut_factory, exceptions, tries=10, delay=1):
    errors = []
    for _ in range(tries):
        fut = fut_factory()
        try:
            r = await fut
            log.info('Future succeeded')
            return r
        except exceptions as e:
            errors.append(e)
            await asyncio.sleep(delay)
            log.info('Retrying %s', e)
    raise RuntimeError("Exhausted: {}".format(errors[0]))


class RabbitSupervisor(object):

    def __init__(self):
        self.queue = None

    async def init_queue(self):
        # connect to the RabbitMQ broker
        connect = lambda: asynqp.connect('rabbitmq', 5672,
                                         username=os.getenv('RABBITMQ_USER', 'yo'),
                                         password=os.getenv('RABBITMQ_PASS', 'yo'))

        connection = await retry_fut(connect, (ConnectionError, ConnectionRefusedError, OSError))

        # Open a communications channel
        channel = await connection.open_channel()

        # Create a queue and an exchange on the broker
        exchange = await channel.declare_exchange('email.exchange', 'direct')
        queue = await channel.declare_queue('emails.queue')
        await queue.bind(exchange, 'routing.key')
        self.queue = queue
        return queue

    async def receive(self):
        while True:
            received_message = await self.queue.get()
            if not received_message:
                await asyncio.sleep(1)
                continue
            return received_message


async def init_db():
    pool_fut = lambda: aiopg.create_pool(os.getenv('DB_URI'))
    pool = await retry_fut(pool_fut, (psycopg2.OperationalError,))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(schema)
    return pool


async def insert(pool, data):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(update_query, data)


async def email_collector(rabbit, pool):
    while True:
        received_message = await rabbit.receive()
        await insert(pool, received_message.json())
        log.debug("Message: %s", received_message.json())
        received_message.ack()


async def spawn(loop):
    rabbit = RabbitSupervisor()
    await rabbit.init_queue()
    pool = await init_db()
    for _ in range(5):
        loop.create_task(email_collector(rabbit, pool))

if __name__ == "__main__":
    if os.getenv('DEBUG'):
        log.setLevel(logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.create_task(spawn(loop))
    loop.run_forever()

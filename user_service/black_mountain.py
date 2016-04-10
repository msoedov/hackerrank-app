import logging
import os

import aiopg
import asyncio
import asynqp
import psycopg2
from retry import retry

schema = """CREATE TABLE IF NOT EXISTS users (
    email VARCHAR(100) NOT NULL,
    fullname VARCHAR(100),
    times_received INTEGER NOT NULL
);
--- CREATE UNIQUE INDEX users_email_uindex ON users (email);
"""

update_query = """
--- Cool kid use ON CONFLICT
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN TRANSACTION;
UPDATE users set times_received = times_received + 1 where email = %(email)s;
INSERT INTO users (email, fullname, times_received) SELECT %(email)s, %(fullname)s, 1
WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = %(email)s);
commit TRANSACTION;
"""


log = logging.getLogger()
log.setLevel(logging.DEBUG)


@retry(ConnectionRefusedError, tries=10, delay=1)
async def init_queue():
    # connect to the RabbitMQ broker
    connection = await asynqp.connect('docker', 5672, username='yo', password='yo')

    # Open a communications channel
    channel = await connection.open_channel()

    # Create a queue and an exchange on the broker
    exchange = await channel.declare_exchange('email.exchange', 'direct')
    queue = await channel.declare_queue('emails.queue')
    await queue.bind(exchange, 'routing.key')
    return queue


@retry(psycopg2.OperationalError)
async def init_db():
    pool = await aiopg.create_pool('postgres://postgres:postgres2016@docker/users')
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(schema)
    return pool


async def insert(pool, data):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(update_query, data)


@asyncio.coroutine
def email_collector(queue, pool):
    while True:
        received_message = yield from queue.get()
        if not received_message:
            yield from asyncio.sleep(1)
            continue
        yield from insert(pool, received_message.json())
        print(received_message.json())  # get JSON from incoming messages easily
        received_message.ack()

async def spawn(loop):
    queue = await init_queue()
    pool = await init_db()
    for _ in range(5):
        loop.create_task(email_collector(queue, pool))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(spawn(loop))
    loop.run_forever()

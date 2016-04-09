import asyncio
import logging
import os
import asynqp
import aiopg
from retry import retry
import psycopg2

schema = """CREATE TABLE IF NOT EXISTS users (
    email VARCHAR(100) NOT NULL,
    fullname VARCHAR(100),
    times_received INTEGER NOT NULL
);
--- CREATE UNIQUE INDEX users_email_uindex ON users (email);
"""

udate_query = """
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


@retry(OSError)
async def init_queue():
    # connect to the RabbitMQ broker
    connection = await asynqp.connect('docker', 5672, username='yo', password='yo')

    # Open a communications channel
    channel = await connection.open_channel()

    # Create a queue and an exchange on the broker
    exchange = await channel.declare_exchange('email.exchange', 'direct')
    queue = await channel.declare_queue('emails.queue')
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
            await cur.execute(udate_query, data)
            await cur.execute('select * from users')
            r = await cur.fetchall()
            log.debug("Got %s", r)



@asyncio.coroutine
def email_collector(queue, pool):
    # queue = yield from init_queue()
    # pool = yield from init_db()
    while True:
        received_message = yield from queue.get()
        yield from insert(pool, {'email': 'test@test', 'fullname': 'Hello'})
        if not received_message:
            log.debug('Pooling...')
            yield from asyncio.sleep(1)
            continue
        print(received_message.json())  # get JSON from incoming messages easily
        received_message.ack()

async def spawn(loop):
    queue = await init_queue()
    pool = await init_db()
    for _ in range(2):
        loop.create_task(email_collector(queue, pool))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(spawn(loop))
    loop.run_forever()

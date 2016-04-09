import json
import consul
import asyncio


async def handle_tcp_client(reader, writer):
    data = await reader.readline()
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print("-> Server received %r from %r" % (message, addr))
    print("<- Server sending: %r" % message)
    writer.write('ack')
    await writer.drain()
    print("-- Terminating connection on server")
    writer.close()

async def register(loop):
    c = consul.Consul('consul').Agent.Service()


def main():
    loop = asyncio.get_event_loop()
    print('Launching loop')
    server_task = asyncio.start_server(handle_tcp_client, '0.0.0.0', port=5757, backlog=5)
    return loop.run_forever()


if __name__ == "__main__":
    main()

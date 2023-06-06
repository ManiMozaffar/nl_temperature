from aiokafka import AIOKafkaConsumer
import asyncio
import json


async def consume():
    consumer = AIOKafkaConsumer(
        'requests',
        bootstrap_servers='127.0.0.1:9092'
    )

    await consumer.start()
    try:
        async for msg in consumer:
            order_event = json.loads(msg.value.decode())
            print("Consumed: ", order_event)
            ...  # make requests here in ASYNC
            # EDD IS AWESOME!
    finally:
        await consumer.stop()

asyncio.run(consume())

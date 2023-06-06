from aiokafka import AIOKafkaConsumer
import asyncio
import json


async def consume():
    consumer = AIOKafkaConsumer(
        'pizza_orders',
        bootstrap_servers='127.0.0.1:9092',
        group_id="my-group")

    await consumer.start()
    try:
        async for msg in consumer:
            order_event = json.loads(msg.value.decode())
            print("Consumed: ", order_event)
    finally:
        await consumer.stop()

asyncio.run(consume())

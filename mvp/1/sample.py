import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

bootstrap_servers = '127.0.0.1:9092'
topic_name = 'my_topic'


async def create_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    return producer


async def create_consumer():
    consumer = AIOKafkaConsumer(
        topic_name, bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    return consumer


async def send_message(producer: AIOKafkaProducer, message):
    try:
        await producer.send_and_wait(topic_name, json.dumps(message))
        print(f"Sent message: {message}")
    except Exception as e:
        print(f"Failed to send message: {e}")


async def handle_message(message):
    print(f"Received message: {message.value}")


async def main():
    producer = await create_producer()
    consumer = await create_consumer()
    consumer_task = asyncio.create_task(consume_messages(consumer, 2))
    await send_message(
        producer, {'id': 1, 'data': 'Hello, Kafka!'}
    )
    await send_message(
        producer, {'id': 2, 'data': 'Another message'}
    )
    await consumer_task
    await producer.stop()
    await consumer.stop()


async def consume_messages(consumer, num_messages):
    count = 0
    async for message in consumer:
        await handle_message(message)
        count += 1
        if count >= num_messages:
            break


if __name__ == '__main__':
    asyncio.run(main())

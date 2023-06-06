from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
import json

app = FastAPI()
producer = AIOKafkaProducer(bootstrap_servers='127.0.0.1:9092')


@app.on_event("startup")
async def startup_event():
    await producer.start()


@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()


@app.post('/pizza_order/{customer_name}/{pizza_type}')
async def pizza_order(customer_name: str, pizza_type: str):
    order_event = {"customer_name": customer_name, "pizza_type": pizza_type}
    await producer.send_and_wait(
        "pizza_orders", json.dumps(order_event).encode()
    )
    return {"message": "Order placed!"}

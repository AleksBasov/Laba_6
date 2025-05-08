from fastapi import FastAPI
import aio_pika
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from tenacity import retry, wait_fixed, stop_after_attempt
import logging
import gateway
import uuid
import asyncio



app = FastAPI()


RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessageRequest(BaseModel):
    message: str
@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)

@app.on_event("startup")
async def startup():
    app.state.connection = await aio_pika.connect_robust(RABBITMQ_URL)
    app.state.channel = await app.state.connection.channel()
    app.state.exchange = await app.state.channel.declare_exchange("messages", aio_pika.ExchangeType.DIRECT)

@app.on_event("shutdown")
async def shutdown():
    await app.state.connection.close()


@app.post("/send/")
async def send_message(request: MessageRequest):
    message = request.message
    print(f"Received message: {message}")
    
    await app.state.exchange.publish(
        aio_pika.Message(body=message.encode()),
        routing_key="service_queue"
    )
    return {"status": "message sent"}

# Событие запуска приложения
@app.on_event("startup")
async def startup():
    try:
        # Попытка подключиться к RabbitMQ
        logger.info("Attempting to connect to RabbitMQ...")
        app.state.connection = await connect_to_rabbitmq()
        app.state.channel = await app.state.connection.channel()
        app.state.exchange = await app.state.channel.declare_exchange(
            "messages", aio_pika.ExchangeType.DIRECT)
        app.state.callback_queue = await app.state.channel.declare_queue(exclusive=True)

        async def on_response(message: aio_pika.IncomingMessage):
            correlation_id = message.correlation_id
            if correlation_id in app.state.futures:
                app.state.futures[correlation_id].set_result(message.body)

        await app.state.callback_queue.consume(on_response)

        app.state.futures = {}

        # Логирование успешного подключения
        logger.info("Successfully connected to RabbitMQ and declared exchange 'messages'")

    except Exception as e:
        # Логирование ошибки
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        # Завершение приложения с выбросом исключения
        raise RuntimeError("Application failed to start due to RabbitMQ connection error")


    # Событие остановки приложения
@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'connection') and app.state.connection:
        await app.state.connection.close()
        logger.info("Disconnected from RabbitMQ")


@app.post("/send/")
async def send_message(request: MessageRequest):
    message = request.message
    correlation_id = str(uuid.uuid4())
    future = asyncio.get_event_loop().create_future()
    app.state.futures[correlation_id] = future
    logger.info(f"Received message: {message}")


    try:
        await app.state.exchange.publish(
            aio_pika.Message(
                body=message.encode(),
                reply_to=app.state.callback_queue.name,
                correlation_id=correlation_id
            ),
            routing_key="service_queue"
        )
    response = await future
    return {"response": response.decode()}

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        raise HTTPException(status_code=500, detail="Failed to send message")
    

    









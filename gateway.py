from fastapi import FastAPI
import aio_pika
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from tenacity import retry, wait_fixed, stop_after_attempt
import logging


app = FastAPI()


RABBITMQ_URL = "admin:admin@localhost:15672"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessageRequest(BaseModel):
    message: str


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
        app.state.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        app.state.channel = await app.state.connection.channel()
        app.state.exchange = await app.state.channel.declare_exchange(
            "messages", aio_pika.ExchangeType.DIRECT
        )
        logger.info("Successfully connected to RabbitMQ and declared exchange 'messages'")
    except Exception as e:
        # Логирование ошибки и завершение приложения
        logger.error(f"Failed to connect to RabbitMQ: {e}")
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
    logger.info(f"Received message: {message}")


    try:
        await app.state.exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key="service_queue"
        )
        return {"status": "message sent"}
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        raise HTTPException(status_code=500, detail="Failed to send message")
    
    
    
    # Функция для подключения к RabbitMQ с повторными попытками
@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)






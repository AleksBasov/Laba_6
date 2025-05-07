import asyncio
import aio_pika
from tenacity import retry, wait_fixed, stop_after_attempt
import logging


RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)



async def main():
    connection = None
    try:
        # Подключение к RabbitMQ
        connection = await connect_to_rabbitmq()
        channel = await connection.channel()


        # Объявление обменника
        exchange = await channel.declare_exchange("messages", aio_pika.ExchangeType.DIRECT)


        # Объявление очереди
        queue = await channel.declare_queue("service_queue", durable=True)
        await queue.bind(exchange, routing_key="service_queue")


        logger.info("Successfully connected to RabbitMQ and declared exchange/queue")


        # Чтение сообщений
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    logger.info(f"Received message: {message.body.decode()}")


    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("Disconnected from RabbitMQ")


if __name__ == "__main__":
    asyncio.run(main())


import asyncio
import aio_pika
import json
from tenacity import retry, wait_fixed, stop_after_attempt
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Настроим провайдер трейсинга
resource = Resource(attributes={"service.name": "Service-trace-app"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

# Настройка логирования
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
        exchange = await channel.declare_exchange(
            "messages",
            aio_pika.ExchangeType.DIRECT
        )

        # Объявление очереди
        queue = await channel.declare_queue("service_queue", durable=True)
        await queue.bind(exchange, routing_key="service_queue")

        logger.info("Successfully connected to RabbitMQ and declared exchange/queue")

        # Чтение сообщений
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        incoming_data = json.loads(message.body.decode())
                        trace_id = incoming_data.get("trace_id")
                        incoming_message = incoming_data.get("message")
                        
                        # Спан на обработку сообщения
                        with tracer.start_as_current_span("service_process_message") as span:
                            span.set_attribute("custom.trace_id", trace_id)
                            logger.info(
                                f"[Service][Trace ID: {trace_id}] Received: {incoming_message}"
                            )

                            # Обработка
                            response_text = f"Processed: {incoming_message.upper()}"

                            if message.reply_to:
                                response_payload = {
                                    "trace_id": trace_id,
                                    "result": response_text
                                }
                                
                                current_span = trace.get_current_span()
                                if current_span:
                                    current_span.set_attribute("response.sent", True)
                                
                                await exchange.publish(
                                    aio_pika.Message(
                                        body=json.dumps(response_payload).encode(),
                                        correlation_id=message.correlation_id
                                    ),
                                    routing_key=message.reply_to
                                )
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

    except Exception as e:
        logger.error(f"RabbitMQ connection error: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("Disconnected from RabbitMQ")

if __name__ == "__main__":
    asyncio.run(main())
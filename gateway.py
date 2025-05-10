from fastapi import FastAPI, HTTPException
from tenacity import retry, wait_fixed, stop_after_attempt
import logging
import json
import asyncio
import aio_pika
import time
from pydantic import BaseModel
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import format_trace_id
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import start_http_server, Counter, Histogram

# Настроим провайдер трейсинга
resource = Resource(attributes={"service.name": "Gateway-trace-app"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
Instrumentator().instrument(app).expose(app)
RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

# Прометей-метрики
MESSAGES_PROCESSED = Counter("messages_processed_total", "Total number of messages processed")
MESSAGES_ERRORS = Counter("messages_processing_errors_total", "Total number of processing errors")
MESSAGE_PROCESSING_TIME = Histogram("message_processing_duration_seconds", "Time spent processing message")

otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageRequest(BaseModel):
    message: str

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)

async def main():
    # Запуск сервера метрик Prometheus
    start_http_server(8001)
    logger.info("Prometheus metrics server started on http://localhost:8001")
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
                    start_time = time.time()
                    try:
                        incoming_data = json.loads(message.body)
                        trace_id = incoming_data.get("trace_id")
                        incoming_message = incoming_data.get("message")


                        with tracer.start_as_current_span("service_process_message") as span:
                            span.set_attribute("custom.trace_id", trace_id)
                            print(f"[Service][Trace ID: {trace_id}] Received: {incoming_message}")


                            # Обработка сообщения
                            response_text = f"Processed: {incoming_message.upper()}"


                            # Отправка ответа, если задан reply_to
                            if message.reply_to:
                                response_payload = {
                                    "trace_id": trace_id,
                                    "result": response_text
                                }
                                await channel.default_exchange.publish(
                                    aio_pika.Message(
                                        body=json.dumps(response_payload).encode(),
                                        correlation_id=message.correlation_id
                                    ),
                                    routing_key=message.reply_to
                                )


                        MESSAGES_PROCESSED.inc()  # Увеличить счётчик успешной обработки


                    except Exception as e:
                        MESSAGES_ERRORS.inc()
                        logger.error(f"Error while processing message: {e}")


                    finally:
                        MESSAGE_PROCESSING_TIME.observe(time.time() - start_time)

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("Disconnected from RabbitMQ")


if __name__ == "__main__":
    asyncio.run(main())


@app.on_event("startup")
async def startup():
    try:
        # Попытка подключиться к RabbitMQ
        logger.info("Attempting to connect to RabbitMQ...")
        app.state.connection = await connect_to_rabbitmq()
        app.state.channel = await app.state.connection.channel()
        app.state.exchange = await app.state.channel.declare_exchange(
            "messages", aio_pika.ExchangeType.DIRECT
        )
        app.state.callback_queue = await app.state.channel.declare_queue(exclusive=True)
        async def on_response(message: aio_pika.IncomingMessage):
            correlation_id = message.correlation_id
            if correlation_id in app.state.futures:
                app.state.futures[correlation_id].set_result(message.body)
      
        await app.state.callback_queue.consume(on_response)


        app.state.futures = {}
        logger.info("Successfully connected to RabbitMQ and declared exchange 'messages'")
        
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise RuntimeError("Application failed to start due to RabbitMQ connection error")

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'connection') and app.state.connection:
        await app.state.connection.close()
        logger.info("Disconnected from RabbitMQ")


@app.post("/send/")
async def send_message(request: MessageRequest):
    try:
        message = request.message
        span = trace.get_current_span()
        trace_id = format_trace_id(span.get_span_context().trace_id)
        logger.info(f"[TRACE_ID] {trace_id}")
        
        payload = {
            "trace_id": trace_id,
            "message": message
        }

        # Создаем Future для ожидания ответа
        future = asyncio.get_event_loop().create_future()
        app.state.futures[trace_id] = future

        logger.info(f"Received message: {message}")

        try:
            with tracer.start_as_current_span("gateway_send_message") as send_span:
                send_span.set_attribute("custom.trace_id", trace_id)
                
                # Отправляем сообщение
                await app.state.exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode(),
                        reply_to=app.state.callback_queue.name,
                        correlation_id=trace_id
                    ),
                    routing_key="service_queue"
                )

            # Ожидаем ответ с таймаутом
            with tracer.start_as_current_span("gateway_wait_response") as wait_span:
                wait_span.set_attribute("custom.trace_id", trace_id)
                try:
                    response = await asyncio.wait_for(future, timeout=30.0)
                    decoded_response = json.loads(response)
                    logger.info(f"[Gateway][Trace ID: {trace_id}] Response: {decoded_response}")
                    return decoded_response
                except asyncio.TimeoutError:
                    logger.error(f"Timeout waiting for response, trace_id: {trace_id}")
                    raise HTTPException(status_code=504, detail="Service response timeout")
                except json.JSONDecodeError:
                    logger.error(f"Invalid response format, trace_id: {trace_id}")
                    raise HTTPException(status_code=502, detail="Invalid response format")

        except aio_pika.exceptions.AMQPError as e:
            logger.error(f"AMQP error: {str(e)}")
            raise HTTPException(status_code=503, detail="Message broker unavailable")
        finally:
            # Очищаем Future в любом случае
            app.state.futures.pop(trace_id, None)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
import asyncio
import aio_pika
import logging
import json
import time
from prometheus_client import start_http_server, Counter, Gauge
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

RABBITMQ_URL = "amqp://admin:admin@localhost:5672"

# Prometheus-метрики
MESSAGES_PROCESSED = Counter("messages_processed_service2_total", "Total number of messages processed by Service 2")
MESSAGE_PROCESSING_RATE = Gauge("message_processing_rate_service2", "Message processing rate (messages per second) in Service 2")

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Настройка OpenTelemetry
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)
otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)

async def main():
    start_http_server(8002)  # Порт для метрик Prometheus
    logger.info("Prometheus metrics server started on http://localhost:8002")

    connection = await connect_to_rabbitmq()
    channel = await connection.channel()

    exchange = await channel.declare_exchange("messages", aio_pika.ExchangeType.DIRECT)
    queue = await channel.declare_queue("service2_queue", durable=True)
    await queue.bind(exchange, routing_key="service2_queue")

    logger.info("Service 2 connected to RabbitMQ")

    async with queue.iterator() as queue_iter:
        message_count = 0
        start_time = time.time()
        async for message in queue_iter:
            async with message.process():
                current_time = time.time()
                message_count += 1

                # Вычисляем скорость обработки сообщений (сообщений в секунду)
                elapsed_time = current_time - start_time
                if elapsed_time > 0:
                    processing_rate = message_count / elapsed_time
                    MESSAGE_PROCESSING_RATE.set(processing_rate)

                try:
                    incoming_data = json.loads(message.body)
                    trace_id = incoming_data.get("trace_id")
                    incoming_message = incoming_data.get("message")

                    with tracer.start_as_current_span("service2_process_message") as span:
                        span.set_attribute("custom.trace_id", trace_id)
                        logger.info(f"[Service 2][Trace ID: {trace_id}] Received: {incoming_message}")

                        # Обработка сообщения
                        response_text = f"Service 2 Processed: {incoming_message.lower()}"

                        # Отправка ответа
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

                    MESSAGES_PROCESSED.inc()

                except Exception as e:
                    logger.error(f"Error while processing message: {e}")

    await connection.close()

if __name__ == "__main__":
    asyncio.run(main())
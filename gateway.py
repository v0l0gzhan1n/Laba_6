from fastapi import FastAPI, HTTPException
import logging
import json
import aio_pika
import uuid
import asyncio
from pydantic import BaseModel
from tenacity import retry, wait_fixed, stop_after_attempt
from opentelemetry.trace import format_trace_id
from prometheus_fastapi_instrumentator import Instrumentator
# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
Instrumentator().instrument(app).expose(app)
RABBITMQ_URL = "amqp://admin:admin@localhost:5672"
# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Модель для запроса через body
class MessageRequest(BaseModel):
    message: str

# Событие запуска приложения
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
        # Логирование ошибки и завершение приложения
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise RuntimeError("Application failed to start due to RabbitMQ connection error")

# Событие остановки приложения
@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'connection') and app.state.connection:
        await app.state.connection.close()
        logger.info("Disconnected from RabbitMQ")


# Конечная точка для отправки сообщений
@app.post("/send/")
async def send_message(request: MessageRequest):
    message = request.message
    correlation_id = str(uuid.uuid4())
    span = trace.get_current_span()
    trace_id = format_trace_id(span.get_span_context().trace_id)
    logger.info(f"[TRACE_ID] {trace_id}")
    payload = {
            "trace_id": trace_id,
            "message": message
        }
    future = asyncio.get_event_loop().create_future()
    app.state.futures[correlation_id] = future
    logger.info(f"Received message: {message}")
    try:
        with tracer.start_as_current_span("gateway_send_message") as send_span:
          send_span.set_attribute("custom.trace_id", trace_id)
          await app.state.exchange.publish(
              aio_pika.Message(
                  body=json.dumps(payload).encode(),
                  reply_to=app.state.callback_queue.name,
                  correlation_id=correlation_id
              ),
              routing_key="service_queue"
          )
        with tracer.start_as_current_span("gateway_wait_response"):
            response = await future
        decoded_response = json.loads(response)
        # Логируем trace_id вместе с ответом
        # print(f"[Gateway][Trace ID: {trace_id}] Response: {decoded_response}")
        logger.info(f"[Gateway][Trace ID: {trace_id}] Response: {decoded_response}")


        return decoded_response

    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        raise HTTPException(status_code=500, detail="Failed to send message")

# Функция для подключения к RabbitMQ с повторными попытками
@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def connect_to_rabbitmq():
    logger.info("Attempting to connect to RabbitMQ...")
    return await aio_pika.connect_robust(RABBITMQ_URL)

# Настроим провайдер трейсинга
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

# Настройка
# otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317//v1/traces", insecure=True)
otlp_exporter = OTLPSpanExporter(endpoint="localhost:4317", insecure=True)


span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
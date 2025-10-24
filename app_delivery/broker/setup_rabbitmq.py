from aio_pika import connect_robust, ExchangeType

RABBITMQ_HOST = "amqp://guest:guest@rabbitmq/"
EXCHANGE_NAME = "order_payment_exchange"

async def setup_rabbitmq():
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.TOPIC,
        durable=True
    )

    order_ready_queue = await channel.declare_queue('order_ready_queue', durable=True)

    await order_ready_queue.bind(exchange, routing_key='order.ready')

    print("✅ RabbitMQ configurado correctamente (exchange + colas creadas).")

    connection.close()
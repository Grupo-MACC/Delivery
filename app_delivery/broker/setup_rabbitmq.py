from microservice_chassis_grupo2.core.rabbitmq_core import get_channel, declare_exchange, declare_exchange_saga, declare_exchange_command

async def setup_rabbitmq():
    connection, channel = await get_channel()
    
    exchange = await declare_exchange()
    exchange_saga = await declare_exchange_saga()
    exchange_command = await declare_exchange_command() 

    order_ready_queue = await channel.declare_queue('order_ready_queue', durable=True)
    await order_ready_queue.bind(exchange, routing_key='order.ready')
    check_delivery_queue = await channel.declare_queue('check_delivery_queue', durable=True)
    await check_delivery_queue.bind(exchange_command, routing_key='check.delivery')

    delivery_queue = await channel.declare_queue('delivery_queue', durable=True)
    await delivery_queue.bind(exchange, routing_key="auth.running")
    await delivery_queue.bind(exchange, routing_key="auth.not_running")

    print("✅ RabbitMQ configurado correctamente (exchange + colas creadas).")

    connection.close()
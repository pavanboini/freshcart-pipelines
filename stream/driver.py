import traceback
import pika
import json
import time

EXCHANGE = "rapido-dispatch"

REQUIRED_FIELDS = [
    "id",
    "city",
    "zone",
    "type",
    "ts"
]


def run_driver(
    amqp_url,
    name,
    pattern,
    stop_event,
    log_queue
):

    connection = None

    queue_name = f"rapido-driver-{name.lower()}"

    try:

        params = pika.URLParameters(amqp_url)
        params.heartbeat = 60
        params.blocked_connection_timeout = 30

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare topic exchange
        channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True
        )

        # Create driver's private queue
        channel.queue_declare(
            queue=queue_name,
            durable=True
        )

        # Bind queue using topic pattern
        channel.queue_bind(
            exchange=EXCHANGE,
            queue=queue_name,
            routing_key=pattern
        )

        # One unacknowledged message at a time
        channel.basic_qos(prefetch_count=1)

        log_queue.put((
            name,
            f"Connected | Pattern: {pattern}"
        ))

        while not stop_event.is_set():

            method, properties, body = channel.basic_get(
                queue=queue_name,
                auto_ack=False
            )

            if method is None:
                stop_event.wait(0.15)
                continue

            try:

                message = json.loads(body.decode("utf-8"))

                missing_fields = [
                    field for field in REQUIRED_FIELDS
                    if field not in message
                    or message[field] is None
                ]

                if missing_fields:

                    channel.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False
                    )

                    log_queue.put((
                        name,
                        f"NACK | ID: {message.get('id', 'Unknown')} "
                        f"| Missing: {missing_fields} "
                        f"| Discarded"
                    ))

                else:

                    channel.basic_ack(
                        delivery_tag=method.delivery_tag
                    )

                    log_queue.put((
                        name,
                        f"ACK | ID: {message['id']} "
                        f"| {message['city']}."
                        f"{message['zone']}."
                        f"{message['type']} "
                        f"| Processed successfully"
                    ))

            except (json.JSONDecodeError, UnicodeDecodeError):

                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )

                log_queue.put((
                    name,
                    "NACK | Invalid JSON | Discarded"
                ))

    except Exception as e:

        error_details = (
            f"ERROR: {type(e).__name__}: {repr(e)}\n"
            f"{traceback.format_exc()}"
        )

        log_queue.put((
            name,
            error_details
        ))

    finally:

        if connection and connection.is_open:
            connection.close()

        log_queue.put((
            name,
            "Driver stopped."
        ))

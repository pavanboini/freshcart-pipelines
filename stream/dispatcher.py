import traceback
import pika
import random
import json
import time

from datetime import datetime, timezone

EXCHANGE = "rapido-dispatch"

CITIES = ["bangalore", "chennai"]

ZONES = [
    "koramangala",
    "indiranagar",
    "whitefield",
    "tnagar"
]

TYPES = ["ride", "food"]


def connect_rabbitmq(amqp_url):

    params = pika.URLParameters(amqp_url)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True
    )

    return connection, channel


def publish_message(channel, message, routing_key):

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2
        )
    )


def run_dispatcher(amqp_url, stop_event, log_queue, bad_every=0):

    connection = None

    try:

        connection, channel = connect_rabbitmq(amqp_url)

        log_queue.put((
            "Dispatcher",
            "Connected to RabbitMQ. Dispatcher started."
        ))

        message_id = 0

        while not stop_event.is_set():

            message_id += 1

            city = random.choice(CITIES)
            zone = random.choice(ZONES)
            request_type = random.choice(TYPES)

            routing_key = f"{city}.{zone}.{request_type}"

            message = {
                "id": message_id,
                "city": city,
                "zone": zone,
                "type": request_type,
                "ts": datetime.now(timezone.utc).isoformat()
            }

            # D3: Send one invalid message every N messages
            is_bad = (
                bad_every > 0
                and message_id % bad_every == 0
            )

            if is_bad:

                del message["zone"]

                log_queue.put((
                    "Dispatcher",
                    f"BAD MESSAGE #{message_id}: Missing zone field"
                ))

            publish_message(
                channel,
                message,
                routing_key
            )

            log_queue.put((
                "Dispatcher",
                f"Published #{message_id} | {routing_key}"
            ))

            stop_event.wait(2)

    except Exception as e:

        error_details = (
            f"ERROR: {type(e).__name__}: {repr(e)}\n"
            f"{traceback.format_exc()}"
        )

        log_queue.put((
            "Dispatcher",
            error_details
        ))

    finally:

        if connection and connection.is_open:
            connection.close()

        log_queue.put((
            "Dispatcher",
            "Dispatcher stopped."
        ))


def publish_d1_test_message(amqp_url):

    connection, channel = connect_rabbitmq(amqp_url)

    routing_key = "bangalore.koramangala.food"

    message = {
        "id": "D1-TEST-001",
        "city": "bangalore",
        "zone": "koramangala",
        "type": "food",
        "ts": datetime.now(timezone.utc).isoformat()
    }

    publish_message(channel, message, routing_key)

    connection.close()

    return routing_key

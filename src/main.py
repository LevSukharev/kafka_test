import json

from settings import settings
import httpx

from confluent_kafka import Producer, Consumer
from datetime import date


def get_user():
    url = "https://randomuser.me/api"
    response = httpx.get(url)

    if response.status_code == 200:
        try:
            user = response.json()["results"][0]
            return json.dumps(user, ensure_ascii=False, indent=4)
        except Exception as e:
            return 0
    else:
        return response.status_code


def delivery_report(err, msg):
    if err is not None:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    else:
        print('Message delivery failed: {}'.format(err))


def produce_msg():
    producer = Producer(
        {'bootstrap.servers': settings.bootstrap_servers,
         'sasl.mechanism': 'SCRAM-SHA-256',
         'security.protocol': 'SASL_SSL',
         'sasl.username': settings.user_kafka,
         'sasl.password': settings.password_kafka}
    )

    data = get_user()

    try:
        producer.produce(topic=settings.topic, value=data.encode('utf-8'), callback=delivery_report)
        producer.flush()

        with open('user.json', 'w', encoding='utf-8') as f:
            f.write(data)

        print("Message produced")

    except Exception as e:
        print(f"Error producing message: {e}")


def consume_msg():
    consumer = Consumer(
        {'bootstrap.servers': settings.bootstrap_servers,
         'sasl.mechanism': 'SCRAM-SHA-256',
         'security.protocol': 'SASL_SSL',
         'sasl.username': settings.user_kafka,
         'sasl.password': settings.password_kafka,
         'group.id': 'CONSUMER_GROUP',
         'auto.offset.reset': 'earliest'
         }
    )

    consumer.subscribe([settings.topic])

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                print("Consumer error: {}".format(message.error()))
                continue

            message = json.dumps(message.value().decode('utf-8'), ensure_ascii=False, indent=4)
            with open(f"user_{date.today().strftime('%d-%m-%Y')}.json", 'w', encoding='utf-8') as f:
                f.write(message)
            break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    consume_msg()

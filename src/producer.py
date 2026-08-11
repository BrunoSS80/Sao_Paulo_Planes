from kafka import KafkaProducer
from opensky_api import OpenSkyApi
import json
import os
import time
from dotenv import load_dotenv


def serialize_states(states):
    return {
        "time": states.time,
        "states": [
            state.__dict__ if hasattr(state, "__dict__") else state
            for state in states.states or []
        ],
    }


def main():
    load_dotenv()
    client_id = os.getenv("ClientId")
    client_secret = os.getenv("ClientSecret")

    api = OpenSkyApi(client_id=client_id, client_secret=client_secret)

    print("-Iniciando o produtor Kafka-")
    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:
        states = api.get_states(bbox=(-24.10, -23.20, -47.20, -46.20))
        payload = serialize_states(states)
        producer.send("plane_data", payload)
        producer.flush()
        print(f"-Mensagem enviada para o tópico plane_data ({len(payload['states'])} states)-")
        time.sleep(30)


if __name__ == "__main__":
    main()
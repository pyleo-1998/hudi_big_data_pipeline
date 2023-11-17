from confluent_kafka import Consumer
from schema_class import Schema_Class
from config import KAFKA_BROKER,KAFKA_TOPIC
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


c = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'consumer_scraper_group_1',
    'auto.offset.reset': 'earliest'
})


c.subscribe([KAFKA_TOPIC])



def process_record_confluent(record: bytes, src: SchemaRegistryClient, schema: str):
    deserializer = AvroDeserializer(schema_str=schema, schema_registry_client=src)
    return deserializer(record, None) # returns dict


while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    schema_class = Schema_Class()
    sr, latest_version = schema_class.get_schema_from_schema_registry()
    print(process_record_confluent(msg.value(),sr,latest_version.schema.schema_str))


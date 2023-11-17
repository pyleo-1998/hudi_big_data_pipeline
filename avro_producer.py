import time 
from scrape_datagenrator import DataGenerator
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from config import KAFKA_BROKER,KAFKA_TOPIC,AVRO_SCHEMA_REGISTRY_URL,SCHEMA_REGISTRY_SUBJECT


def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))

def avro_producer( kafka_url, schema_registry_url, schema_registry_subject):

    sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)


    value_avro_serializer = AvroSerializer(schema_registry_client = sr,
                                          schema_str = latest_version.schema.schema_str,
                                          conf={
                                              'auto.register.schemas': False
                                            }
                                          )

    producer = SerializingProducer({
        'bootstrap.servers': kafka_url,
        'security.protocol': 'plaintext',
        'value.serializer': value_avro_serializer,
        'delivery.timeout.ms': 120000, # set it to 2 mins
        'enable.idempotence': 'true'
    })

    try:
        
        for _ in range(200):

            for i in range(1,20):
                columns =  ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts"]
                data_list = DataGenerator.get_data()
                json_data = dict(
                    zip
                    (
                        columns,data_list
                    )
                )

                producer.produce(topic=KAFKA_TOPIC, value=json_data, on_delivery=delivery_report)

                events_processed = producer.poll(1)
                print(f"events_processed: {events_processed}")

                messages_in_queue = producer.flush(1)
                print(f"messages_in_queue: {messages_in_queue}")
        
                time.sleep(2)
                
    except Exception as e:
        print(e)


def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    return sr, latest_version

if __name__ == '__main__':
    avro_producer( KAFKA_BROKER, AVRO_SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_SUBJECT)

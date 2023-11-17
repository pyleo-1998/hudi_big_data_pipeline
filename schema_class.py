import json
from schema_for_project import schema
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaRegistryClient
from config import KAFKA_TOPIC,AVRO_SCHEMA_REGISTRY_URL,SCHEMA_REGISTRY_SUBJECT


class Schema_Class():
    
    def __init__(self) -> None:
        self.kafk_topic = KAFKA_TOPIC
        self.schema_registry_url = AVRO_SCHEMA_REGISTRY_URL
        self.schema_registry_subject = SCHEMA_REGISTRY_SUBJECT

    def get_schema_from_schema_registry(self):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        latest_version = sr.get_latest_version(self.schema_registry_subject)
        return sr, latest_version

    def register_schema(self, schema_str):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = sr.register_schema(subject_name=self.schema_registry_subject, schema=schema)
        return schema_id

    def update_schema(self, schema_str):
        sr = SchemaRegistryClient({'url': self.schema_registry_url})
        versions_deleted_list = sr.delete_subject(self.schema_registry_subject)
        print(f"versions of schema deleted list: {versions_deleted_list}")
        schema_id = self.register_schema(schema_str)
        return schema_id


if __name__ == '__main__':
    
    schema_class = Schema_Class()
    schema_id = schema_class.register_schema( json.dumps(schema))
    print(schema_id)

    sr, latest_version = schema_class.get_schema_from_schema_registry()
    print(latest_version.schema.schema_str)
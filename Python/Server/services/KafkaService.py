import json
import os
from typing import Any, NoReturn, Dict

from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from Server.utils.utils import read_json_configs, root_path, info, warn

from Server.domains.PBMParams import DTOResponse


class PBMKafkaService:
    def __init__(self):
        path_to_kafka_confs: str = os.path.join(root_path, "configs/kafka-configs.json")
        self.configs: Dict[str, Any] = read_json_configs(path_to_kafka_confs)
        self.producer: KafkaProducer = KafkaProducer(bootstrap_servers=self.configs.get("bootstrap_servers"),
                                                     client_id=self.configs.get("client_id"),
                                                     value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                                     acks=self.configs.get("acknowledgments"))

    def write_to_pbm_topic(self, data: Dict[str, Any]) -> NoReturn:
        response: DTOResponse = DTOResponse(data)
        if response.valid():
            self.write_to_kafka(self.configs.get("pbm_topic"), response.to_json())
        else:
            warn(f"JSON response {response.to_json()} is not valid.")

    def write_to_kafka(self, topic: str, data: Any) -> NoReturn:
        meta: FutureRecordMetadata = self.producer.send(topic=topic, value=data)
        sent = meta.get()
        if sent:
            info(f"{data} successfully sent to the {topic} topic.")
        else:
            warn(f"Error occurred while sending {data} to the {topic} topic.")

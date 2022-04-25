from typing import Dict, Any

from flask import Flask
from flask import request

from Server.services.KafkaService import PBMKafkaService
from Server.services.PageService import PageService

app = Flask(__name__)
page_service: PageService = PageService()
kafka_service: PBMKafkaService = PBMKafkaService()


@app.route('/api/v1/getPages')
def get_pages():
    query: str = request.args.get("query", type=str, default="")
    return page_service.generate_response(query).to_json()


@app.route('/api/v1/submitResult', methods=['POST'])
def submit_result():
    print(1)
    body: Dict[str, Any] = request.get_json()
    kafka_service.write_to_pbm_topic(body)
    return "success"


if __name__ == '__main__':
    app.run()

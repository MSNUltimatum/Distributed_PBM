import os.path
import uuid
import hashlib
from typing import List

from Server.utils.utils import root_path
from Server.domains.PBMParams import PagesProbabilityContainer, DTOResult
from Server.repositoreis.PBMRepository import PBMRepository


class PageService:
    def __init__(self):
        path_to_psql_confs: str = os.path.join(root_path, "configs/postgres-configs.json")
        self.repository: PBMRepository = PBMRepository(path_to_psql_confs)

    def generate_response(self, query: str) -> DTOResult:
        query_id: int = self.get_query_id(query)
        pages: List[int] = self.generate_pages(query_id)
        prob_container: PagesProbabilityContainer = self.repository.get_pages_probabilities(query_id, pages)
        results: List[int] = prob_container.get_sorted_results()
        return DTOResult(query_id, results)

    @staticmethod
    def get_query_id(query: str) -> int:
        m = hashlib.md5(query.encode("utf-8"))
        return uuid.UUID(m.hexdigest()).int % 10000

    @staticmethod
    def generate_pages(query_id: int):
        return list(range(query_id, query_id + 10))

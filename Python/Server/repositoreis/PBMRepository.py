import psycopg2

from typing import Dict, Any, List, Tuple
from Server.utils.utils import read_json_configs
from Server.domains.PBMParams import PagesProbabilityContainer


class PBMRepository:
    def __init__(self, path_to_configs: str):
        configs: Dict[str, Any] = read_json_configs(path_to_configs)
        self.connection = self._get_postgres_connection(configs)

    def get_pages_probabilities(self, query_id: int, results: List[int]):
        query: str = f"""
        WITH qp AS (SELECT {query_id} AS q, unnest(array[{','.join(map(str, results))}]) AS r),
        qpr AS (SELECT qp.q, qp.r, row_number() over () AS rn FROM qp)
        SELECT qpr.q, qpr.r, qpr.rn, a."attrNumerator", a."attrDenominator", e."examNumerator", e."examDenominator" 
        FROM qpr
        LEFT JOIN msndb.modelresults.attractiveparams AS a 
            on a.query = qpr.q AND a."resultId" = qpr.r
        LEFT JOIN msndb.modelresults.examinationparams AS e 
            on qpr.rn = e.rank
        """
        data: List[Tuple] = self.execute_query(query)
        return PagesProbabilityContainer(data)

    def execute_query(self, query: str) -> List[Tuple]:
        with self.connection.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
        return data

    @staticmethod
    def _get_postgres_connection(configs: Dict[str, Any]) -> psycopg2.connect:
        host: str = configs["host"]
        database: str = configs["database"]
        user: str = configs["user"]
        password: str = configs["password"]
        return psycopg2.connect(host=host,
                                database=database,
                                user=user,
                                password=password)

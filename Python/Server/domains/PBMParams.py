from typing import Dict, Any, List, Tuple


class Page:
    def __init__(self, elem: Tuple):
        self.query: int = elem[0]
        self.result: int = elem[1]
        self.rank: int = elem[2]
        self.attractiveNumerator: float = elem[3]
        self.attractiveDenominator: int = elem[4]
        self.examinationNumerator: float = elem[5]
        self.examinationDenominator: int = elem[6]

    def get_full_page_probability(self) -> float:
        if self.attractiveNumerator and self.examinationNumerator:
            return ((self.attractiveNumerator / self.attractiveDenominator) *
                    (self.examinationNumerator / self.examinationDenominator))
        else:
            return 0.0


class DTOResult:
    def __init__(self, query: int, results: List[int]):
        self.query: int = query
        self.results: List[int] = results

    def to_json(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "results": self.results
        }


class DTOResponse:
    def __init__(self, response: Dict[str, Any]):
        self.response: Dict[str, Any] = response
        self.query: int = response.get("query")
        self.results: List[Dict[str, int]] = response.get("results", [])

    def to_json(self) -> Dict[str, Any]:
        return self.response

    def valid(self) -> bool:
        return self.query and all('resultId' in x and 'clicked' in x for x in self.results)


class PagesProbabilityContainer:
    def __init__(self, data: List[Tuple]):
        self.pages: List[Page] = list(map(Page, data))

    def get_sorted_results(self) -> List[int]:
        probabilities: List[float] = list(map(lambda x: x.get_full_page_probability(), self.pages))
        results: List[int] = self.get_results()
        zipped = zip(results, probabilities)
        sorted_results = sorted(zipped, key=lambda x: x[1], reverse=True)
        return list(map(lambda p: p[0], sorted_results))

    def get_results(self) -> List[int]:
        return list(map(lambda x: x.result, self.pages))

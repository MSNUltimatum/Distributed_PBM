import os.path
import time
import random
import requests
from urllib.parse import urlencode, quote
from typing import Dict, Any, List, NoReturn, Union, Set, Callable, Tuple

from requests import Response

from Server.utils.utils import read_json_configs, info, error


class Rule:
    def __init__(self, rule_dict: Dict[str, Any]):
        self.rule_dict: Dict[str, Any] = rule_dict
        self.rule_name: str = rule_dict["name"]
        self.click_probabilities: List[Dict[str, Any]] = rule_dict["click_probabilities"]
        self.queries_parts: List[str] = rule_dict.get("queries_parts", ["hello", "world"])
        self.validate_probabilities()

    def get_query(self):
        words_count: int = random.randint(1, len(self.queries_parts))
        query_in_words: List[str] = random.sample(self.queries_parts, words_count)
        random.shuffle(query_in_words)
        query: str = ' '.join(query_in_words)
        return urlencode({"query": query}, quote_via=quote)

    def validate_probabilities(self) -> NoReturn:
        args_valid: bool = all("probability" in x and "positions" in x for x in self.click_probabilities)
        probs_valid: bool = all(0 < x["probability"] <= 100 for x in self.click_probabilities)
        counts_valid: bool = all(('count' in x and
                                  type(x['positions']) == list and
                                  x['count'] <= len(x['positions'])) or
                                 ('count' not in x)
                                 for x in self.click_probabilities)
        if not args_valid:
            raise Exception("All click probabilities must have probability and positions arguments.")
        elif not probs_valid:
            raise Exception("Probability should be greater than 0 and less or equals than 100.")
        elif not counts_valid:
            raise Exception("Count field should be specified in case of multiple positions and "
                            "should be less or equals than positions length.")

    def get_click_positions(self) -> Set[int]:
        results: Set[int] = set()
        for rule in self.click_probabilities:
            prob: float = rule["probability"]
            positions: Union[List[int], int] = rule["positions"]
            positions: List[int] = positions if type(positions) == list else [positions]
            count: int = rule.get("count", 1)
            if self.check_case(prob):
                results.update(random.sample(positions, count))
        return results

    @staticmethod
    def check_case(prob: float) -> bool:
        case: int = random.randint(0, 100)
        return prob >= case


class Generator:
    def __init__(self, rule_name: str):
        rule_path: str = os.path.join("rules", f"{rule_name}.json")
        rule_dict: Dict[str, Any] = read_json_configs(rule_path)
        self.generator_rules: Rule = Rule(rule_dict)
        info(
            f"\nStrategy name: {self.generator_rules.rule_name}.\nQuery parts: {self.generator_rules.queries_parts}\nClick "
            f"rules: {self.generator_rules.click_probabilities}")

    def start_generating(self, sleep_time_secs: int) -> NoReturn:
        while True:
            self.execute_iteration()
            time.sleep(sleep_time_secs)

    def execute_iteration(self):
        query: str = self.generator_rules.get_query()
        info(f"Query: {query}")
        results: Dict[str, Any] = self.get_result_pages(query)
        info(f"Server response: {results}")
        clicks: List[Dict[str, int]] = self.get_clicks(results["results"])
        info(f"Chosen click distribution: {clicks}")
        self.send_response(results["query"], clicks)

    def get_result_pages(self, query) -> Dict[str, Any]:
        url: str = "http://localhost:5000/api/v1/getPages?" + query
        response: Response = requests.get(url)
        self.raise_response(response, f"querying {url}")
        return response.json()

    def get_clicks(self, results: List[int]) -> List[Dict[str, Any]]:
        click_positions: Set[int] = self.generator_rules.get_click_positions()
        clicked_distribution: Callable[[Tuple[int, int]],
                                       Dict[str, Any]] = lambda x: {"resultId": x[0],
                                                                    "clicked": x[1] in click_positions}
        return list(map(clicked_distribution, zip(results, range(1, len(results) + 1))))

    def send_response(self, query: int, clicks: List[Dict[str, int]]) -> NoReturn:
        url: str = "http://localhost:5000/api/v1/submitResult"
        final_result: Dict[str, Any] = {"query": query, "results": clicks}
        response: Response = requests.post(url, json=final_result)
        self.raise_response(response, f"querying {url} with parameters {final_result}")

    @staticmethod
    def raise_response(response: Response, step: str) -> NoReturn:
        try:
            response.raise_for_status()
        except Exception as e:
            error(f"Error has occurred on step: {step}.\nError trace: {e}.")


g = Generator("last-three-rule")
g.start_generating(2)

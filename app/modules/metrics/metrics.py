from abc import abstractmethod
from typing import Dict, List

class Metrics:
    def __init__(self):
        pass

    @abstractmethod
    def compute_metrics(self, lst_y_test: List, lst_y_pred: List) -> Dict:
        pass

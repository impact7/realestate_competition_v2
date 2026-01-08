
from app.modules.models.lightgbm.lightgbm_regressor_model import LightgbmRegressorModel
from app.modules.metrics.metrics import Metrics
from app.modules.metrics.regressor_metrics import RegressorMetrics

class MetricsBuilder:
    def __init__(self):
        pass

    @classmethod
    def create_metrics(cls, str_model_name: str) -> Metrics:

        if str_model_name == LightgbmRegressorModel.get_model_name():
            return RegressorMetrics()
        else:
            raise ValueError(f"Unknown model name: {str_model_name}")





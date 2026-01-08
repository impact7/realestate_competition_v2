from app.modules.models.model import Model
from app.modules.models.lightgbm.lightgbm_regressor_model import LightgbmRegressorModel

class ModelBuilder:
    @classmethod
    def create_model(cls, str_model_name: str) -> Model:

        if str_model_name == LightgbmRegressorModel.get_model_name():
            return LightgbmRegressorModel()
        else:
            raise ValueError(f"Unknown model name: {str_model_name}")

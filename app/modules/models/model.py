import shap
import pandas as pd
import numpy as np
from typing import Dict, List
from abc import abstractmethod
from app.modules.settings.settings import LST_REMOVE_MODEL_PARAMS
from joblib import Parallel, delayed

class Model:
    def __init__(self):
       self.model = None

    @abstractmethod
    def build_model(self, dict_model_params: Dict, X_train: pd.DataFrame, y_train: np.ndarray,
                    X_test: pd.DataFrame, y_test: np.ndarray,
                   info_train: pd.DataFrame=None, log1p_flag: bool=False):

        raise NotImplementedError()

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        raise NotImplementedError()

    @abstractmethod
    def shap_values(self, X: pd.DataFrame, exact_flag: bool, n_jobs: int=1) -> pd.DataFrame:
        raise NotImplementedError()

    def get_model(self):
        return self.model

    def save_image(self, str_image_path: str, lst_feature_names: List) -> None:
        raise NotImplementedError()

    @classmethod
    def get_model_name(cls):
        return cls.__name__

    def remove_model_params(self, dict_model_params : Dict) -> Dict:
        lst_model_params_keys = list(dict_model_params.keys())

        for key in lst_model_params_keys:
            if key in LST_REMOVE_MODEL_PARAMS:
                del dict_model_params[key]

        return dict_model_params

import logging
import pandas as pd
import numpy as np
import lightgbm as lgb
from typing import Dict
from app.modules.models.model import Model

logger = logging.getLogger(__name__)

class LightgbmModel(Model):

    def __init__(self):
        super().__init__()

    def build_model(self, dict_model_params: Dict, X_train: pd.DataFrame, y_train: np.ndarray,
                    X_test: pd.DataFrame, y_test: np.ndarray,
                    info_train: pd.DataFrame = None, log1p_flag: bool = False,
                    adjust_amount: float=1.0):
        self.log1p_flag = log1p_flag
        self.adjust_amount = adjust_amount

        y_train = y_train * 1.0 / adjust_amount

        if self.log1p_flag:
            y_train = np.log1p(y_train)
            y_test = np.log1p(y_test)

        dict_model_params_copied = dict_model_params.copy()
        dict_model_params_copied = self.remove_model_params(dict_model_params_copied)

        if info_train is not None and (
                'weight' in info_train.columns.tolist()
                or 'weight2' in info_train.columns.tolist()
                or 'weight3' in info_train.columns.tolist()
                or 'log_weight' in info_train.columns.tolist()):

            if 'weight' in info_train.columns.tolist():
                str_weight_column = 'weight'
            elif 'weight2' in info_train.columns.tolist():
                str_weight_column = 'weight2'
            elif 'weight3' in info_train.columns.tolist():
                str_weight_column = 'weight3'
            else:
                str_weight_column = 'log_weight'


            lgb_dataset = lgb.Dataset(X_train, y_train, weight=info_train[str_weight_column])
            lgb_valid = lgb.Dataset(X_test, y_test)
        else:
            lgb_dataset = lgb.Dataset(X_train, y_train)
            lgb_valid = lgb.Dataset(X_test, y_test)

        num_boost_round = int(dict_model_params_copied['num_boost_round'])
        del dict_model_params_copied['num_boost_round']

        if 'early_stopping_rounds' in dict_model_params_copied:
            early_stopping_rounds = int(dict_model_params_copied['early_stopping_rounds'])
            del dict_model_params_copied['early_stopping_rounds']
        else:
            early_stopping_rounds = None

        if early_stopping_rounds is None:
            self.model = lgb.train(dict_model_params_copied, lgb_dataset, num_boost_round=num_boost_round)
        else:
            self.model = lgb.train(dict_model_params_copied,
                                   train_set=lgb_dataset,
                                   num_boost_round=num_boost_round,
                                   valid_sets=[lgb_valid],
                                   valid_names=['valid'],
                                   callbacks=[
                                       lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=False),
                                       lgb.log_evaluation(1)
                                   ]
                                   )

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return self.model.predict(X.loc[:, self.model.feature_name()])

    def get_model(self):
        return self.model





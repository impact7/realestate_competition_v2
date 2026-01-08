import numpy as np
from app.modules.metrics.metrics import Metrics
from typing import Dict, List
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

class RegressorMetrics(Metrics):
    def __init__(self):
        super().__init__()

    def compute_metrics(self, lst_y_test: List, lst_y_pred: List) -> Dict:
        dict_metrics = {}

        lst_mae_score = []
        lst_rmse_score = []
        lst_mape_score = []

        for y_test, y_pred in zip(lst_y_test, lst_y_pred):
            mae_score = mean_absolute_error(y_test, y_pred)
            rmse_score = np.sqrt(mean_squared_error(y_test, y_pred))
            mape_score = mean_absolute_percentage_error(y_test, y_pred)

            lst_mae_score.append(mae_score)
            lst_rmse_score.append(rmse_score)
            lst_mape_score.append(mape_score)

        dict_metrics['mae'] = np.mean(lst_mae_score)
        dict_metrics['rmse'] = np.mean(lst_rmse_score)
        dict_metrics['mape'] = np.mean(lst_mape_score)

        return dict_metrics


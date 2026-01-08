import shap
import numpy as np
import pandas as pd
from app.modules.models.lightgbm.lightgbm_model import LightgbmModel

class LightgbmRegressorModel(LightgbmModel):

    def __init__(self):
        super().__init__()

    def shap_values(self, X: pd.DataFrame, exact_flag: bool = True, n_jobs: int=-1) -> pd.DataFrame:
        explainer = shap.TreeExplainer(self.model)
        shap_values = explainer.shap_values(X)

        return pd.DataFrame(shap_values, columns=X.columns)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if self.log1p_flag:
            y_pred = np.expm1(self.model.predict(X.loc[:, self.model.feature_name()])) * self.adjust_amount
        else:
            y_pred = self.model.predict(X.loc[:, self.model.feature_name()]) * self.adjust_amount

        return y_pred

    def get_model(self):
        return self.model





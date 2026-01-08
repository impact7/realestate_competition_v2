import logging
from typing import List, Dict
from app.modules.utils.seed import SeedUtil
from app.modules.models.model import Model
from app.modules.datamart.cv_ml_datamart import CVMLDatamart
from app.modules.models.model_builder import ModelBuilder

logger = logging.getLogger(__name__)

class CVModels:
    def __init__(self):
        self.lst_models: List[Model] = []

    def append(self, model: Model):
        self.lst_models.append(model)

    def get_models(self) -> List[Model]:
        return self.lst_models

    def get_folds(self):
        return len(self.lst_models)

    def get_model_name(self) -> str:
        return self.lst_models[0].get_model_name()

    def build_cv_models(self, dict_model_params: Dict, cv_ml_datamart: CVMLDatamart) -> CVMLDatamart:

        for ml_datamart in cv_ml_datamart.get_lst_ml_datamart():
            SeedUtil.get_instance().seed_everything()

            X_train, X_test, y_train, y_test, info_train, info_test = ml_datamart.get_data_for_build_model()

            model = ModelBuilder.create_model(dict_model_params['model_name'])
            model.build_model(dict_model_params, X_train, y_train,
                              X_test, y_test, info_train,
                              ml_datamart.get_log1p_flag(), ml_datamart.get_adjust_amount())

            y_pred = model.predict(X_test)
            ml_datamart.set_predict(y_pred)

            self.lst_models.append(model)

        return cv_ml_datamart





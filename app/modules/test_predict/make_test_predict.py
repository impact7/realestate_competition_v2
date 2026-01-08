import logging
import pandas as pd
from typing import List
from app.modules.cv_models.cv_models import CVModels

logger = logging.getLogger(__name__)

class MakeTestPredict:
    @classmethod
    def create(cls) -> 'MakeTestPredict':
        return MakeTestPredict()

    def make_test_predict(self,
                          cv_model: CVModels,
                          df: pd.DataFrame,
                          lst_feature_column_names: List,
                          lst_drop_column_names: List,
                          lst_id_column_names: List) -> pd.DataFrame:

        lst_use_feature_column_names = self._make_lst_feature_column_names(
            df.columns.tolist(), lst_feature_column_names, lst_drop_column_names
        )

        df_ids = df.loc[:, lst_id_column_names].copy()

        X = df.loc[:, lst_use_feature_column_names]

        df_pred = pd.DataFrame()

        for model in cv_model.lst_models:
            df_pred_temp = df_ids.copy()

            df_pred_temp['y_pred'] = model.predict(X)

            df_pred = pd.concat([df_pred, df_pred_temp], axis=0).reset_index(drop=True)

        df_pred = df_pred.groupby(lst_id_column_names).mean().reset_index()

        return df_pred

    def _make_lst_feature_column_names(self, lst_column_names: List, lst_feature_column_names: List,
                                       lst_drop_column_names: List) -> List:

        if lst_feature_column_names is None:
            lst_feature_column_names = lst_column_names

        if lst_drop_column_names is not None:
            for drop_column_name in lst_drop_column_names:
                if drop_column_name in lst_feature_column_names:
                    lst_feature_column_names.remove(drop_column_name)

        # foldは消す、残すか？
        if 'fold_no' in lst_feature_column_names:
            lst_feature_column_names.remove('fold_no')

        return lst_feature_column_names

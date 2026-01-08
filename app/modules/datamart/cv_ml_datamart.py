import logging
import pandas as pd
from typing import List
from app.modules.utils.seed import SeedUtil
from app.modules.datamart.ml_datamart import MLDatamart
from sklearn.model_selection import StratifiedKFold, StratifiedGroupKFold

logger = logging.getLogger(__name__)

class CVMLDatamart:
    def __init__(self):
        self.lst_ml_datamart : List[MLDatamart] = []
        self.lst_info_column_names = None
        self.lst_id_column_names = None
        self.lst_group_column_names = None
        self.n_folds = None

    def make_cv_ml_datamart(self, df: pd.DataFrame, str_target_column_name: str,
                         lst_feature_column_names: List, lst_drop_column_names: List,
                         lst_info_column_names: List, lst_id_column_names: List,
                         lst_group_column_names: List, n_folds: int,
                         fold_no_flg: bool = False,
                         log1p_flag: bool=False,
                         adjust_amount: float=1.0):

        SeedUtil.get_instance().seed_everything()

        self.lst_info_column_names = lst_info_column_names
        self.lst_id_column_names = lst_id_column_names
        self.lst_group_column_names = lst_group_column_names
        self.n_folds = n_folds

        logger.info(adjust_amount)

        if not fold_no_flg:
            df = self._make_folds(df, str_target_column_name, n_folds, lst_group_column_names)

        for fold_no in range(n_folds):
            ml_datamart = MLDatamart()

            ml_datamart.make_ml_datamart(
                df.loc[df['fold_no'] != fold_no, :],
                df.loc[df['fold_no'] == fold_no, :],
                str_target_column_name,
                lst_feature_column_names,
                lst_drop_column_names,
                lst_info_column_names,
                lst_id_column_names,
                log1p_flag,
                adjust_amount
            )

            self.lst_ml_datamart.append(ml_datamart)

    def _make_folds(self, df: pd.DataFrame, str_stratified_column_name: str,
                    n_folds: int, lst_group_column_names: List = None):
        if lst_group_column_names is None:
            sk = StratifiedKFold(n_splits=n_folds, shuffle=True)

            for fold_no, (_, test_idx) in enumerate(sk.split(df, df[str_stratified_column_name])):
                df.loc[test_idx, 'fold_no'] = fold_no
        else:
            sgk = StratifiedGroupKFold(n_splits=n_folds, shuffle=True)
            groups = df[lst_group_column_names]

            for fold_no, (_, test_idx) in enumerate(sgk.split(df, df[str_stratified_column_name], groups)):
                df.loc[test_idx, 'fold_no'] = fold_no

        return df

    def get_lst_ml_datamart(self) -> List[MLDatamart]:
        return self.lst_ml_datamart

    def get_test_and_predict(self):
        """
        修正後の結果を取得する
        """
        lst_y_test = []
        lst_y_pred = []

        for ml_datamart in self.lst_ml_datamart:
            y_test, y_pred = ml_datamart.get_test_and_predict()

            lst_y_test.append(y_test)
            lst_y_pred.append(y_pred)

        return lst_y_test, lst_y_pred

    def get_lst_X_test(self) -> List[pd.DataFrame]:
        lst_X_test = []

        for ml_datamart in self.lst_ml_datamart:
            X_test = ml_datamart.get_X_test()

            lst_X_test.append(X_test)

        return lst_X_test

    def get_lst_ids_test(self) -> List[pd.DataFrame]:
        lst_ids_test = []

        for ml_datamart in self.lst_ml_datamart:
            ids_test = ml_datamart.get_ids_test()

            lst_ids_test.append(ids_test)

        return lst_ids_test

    def get_lst_id_column_names(self) -> List:
        return self.lst_id_column_names

    def get_lst_feature_names(self) -> List:
        return self.lst_ml_datamart[0].get_lst_feature_names()

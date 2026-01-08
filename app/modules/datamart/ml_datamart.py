import logging
import numpy as np
import pandas as pd
from typing import List, Tuple
from app.modules.utils.seed import SeedUtil
from app.modules.settings.settings import EXPM1_CLIP_MIN, EXPM1_CLIP_MAX

logger = logging.getLogger(__name__)

class MLDatamart:
    def __init__(self):
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.info_train = None # lightgbmのランキング学習のgroup、catboostのweightなどを指定する
        self.info_test = None # lightgbmのランキング学習のgroup、catboostのweightなどを指定する
        self.y_train_pred = None
        self.y_test_pred = None
        self.ids_train = None
        self.ids_test = None
        self.lst_feature_column_names = None
        self.lst_id_column_names = None
        self.lst_info_data_column_names = None # lightgbmのランキング学習のgroup、catboostのweightなどを指定する
        self.str_target_column_name = None

    def make_ml_datamart(self, df_train: pd.DataFrame,
                         df_test: pd.DataFrame,
                         str_target_column_name: str,
                         lst_feature_column_names: List=None,
                         lst_drop_column_names: List=None,
                         lst_info_data_column_names: List = None,
                         lst_id_column_names: List=None,
                         log1p_flag: bool=False,
                         adjust_amount: float=1.0):

        SeedUtil.get_instance().seed_everything()

        self.lst_feature_column_names = self._make_lst_feature_column_names(
            df_train.columns.tolist(), lst_feature_column_names, lst_drop_column_names
        )

        self.lst_id_column_names = lst_id_column_names
        self.lst_info_data_column_names = lst_info_data_column_names
        self.str_target_column_name = str_target_column_name

        if df_train is not None:
            self.X_train = df_train.loc[:, self.lst_feature_column_names].reset_index(drop=True)
            self.y_train = df_train.loc[:, self.str_target_column_name].to_numpy()

        if df_test is not None:
            self.X_test = df_test.loc[:, self.lst_feature_column_names].reset_index(drop=True)
            self.y_test = df_test.loc[:, self.str_target_column_name].to_numpy()

        if df_train is None and df_test is None:
            raise ValueError('df_train or df_test is None')

        if lst_id_column_names is not None:
            if df_train is not None:
                self.ids_train = df_train.loc[:, self.lst_id_column_names].reset_index(drop=True)

            if df_test is not None:
                self.ids_test = df_test.loc[:, self.lst_id_column_names].reset_index(drop=True)

        if lst_info_data_column_names is not None:
            if df_train is not None:
                self.info_train = df_train.loc[:, self.lst_info_data_column_names].reset_index(drop=True)

            if df_test is not None:
                self.info_test = df_test.loc[:, self.lst_info_data_column_names].reset_index(drop=True)

        self.log1p_flag = log1p_flag
        self.adjust_amount = adjust_amount

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

    def set_predict(self, y_test_pred: np.ndarray, y_train_pred: np.ndarray=None) -> None:
        self.y_test_pred = y_test_pred
        self.y_train_pred = y_train_pred

    def get_data_for_build_model(self) -> Tuple[pd.DataFrame, pd.DataFrame,
                                                np.ndarray, np.ndarray, pd.DataFrame, pd.DataFrame]:
        """
        log1p等の変換後の数値を提供(y_trainのみ)
        :return:
        """
        return self.X_train, self.X_test, self.y_train, self.y_test, self.info_train, self.info_test

    def get_test_and_predict(self):
        """
        修正後の結果を取得
        :return:
        """

        return self.y_test, self.y_test_pred

    def get_log1p_flag(self):
        return self.log1p_flag

    def get_adjust_amount(self):
        return self.adjust_amount

    def get_X_test(self):
        return self.X_test

    def get_ids_test(self):
        return self.ids_test

    def get_lst_feature_names(self):
        return self.lst_feature_column_names
import pandas as pd
from app.modules.datamart.cv_ml_datamart import CVMLDatamart


class MakePredict:
    @classmethod
    def create(cls) -> 'MakePredict':
        return MakePredict()

    def make_predict(self, cv_ml_datamart: CVMLDatamart) -> pd.DataFrame:
        lst_ids = cv_ml_datamart.get_lst_ids_test()
        _, lst_y_pred = cv_ml_datamart.get_test_and_predict()

        df_pred = pd.DataFrame()

        for ids, y_pred in zip(lst_ids, lst_y_pred):
            df_pred_temp = ids.copy()
            df_pred_temp['y_pred'] = y_pred

            df_pred = pd.concat([df_pred, df_pred_temp], axis=0).reset_index(drop=True)

        return df_pred

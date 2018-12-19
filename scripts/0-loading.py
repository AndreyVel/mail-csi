import os
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import RandomOverSampler
from imblearn.over_sampling import SMOTE

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

os.chdir('/home/avel/work-git/mail-csi-git/scripts')

class DataXyz:
    def __init__(self, code=None, x=None, y=None, z=None):
        self.code = code
        self.x = x
        self.y = y
        self.z = z
        
class RawData:
    def __init__(self, dataPath):
        self.ds_dict = {}
        self.data_dict = {}
        self.voice_dict = {}

        self.features = pd.read_csv(dataPath + 'features.csv', sep=';', index_col=False)
        self.data_dict['cons'] = pd.read_csv(dataPath + 'consumption.csv', sep=';', index_col=False)

        if 1 == 1:
            self.data_dict['dd-ses'] = pd.read_csv(dataPath + 'data_session.csv', sep=';', index_col=False)
            self.data_dict['dd-a06'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data6.csv', sep=';', index_col=False)
            self.data_dict['dd-a13'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data13.csv', sep=';', index_col=False)
            self.data_dict['dd-a19'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data19.csv', sep=';', index_col=False)
            self.data_dict['dd-a20'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data20.csv', sep=';', index_col=False)
            self.data_dict['dd-a21'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data21.csv', sep=';', index_col=False)
            self.data_dict['dd-a24'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data24.csv', sep=';', index_col=False)
            self.data_dict['dd-a29'] = pd.read_csv(dataPath + 'pca-avg-data/pca-avg-data29.csv', sep=';', index_col=False)
            self.data_dict['dd-c13'] = pd.read_csv(dataPath + 'pca-chnn-data/pca-chnn-data13.csv', sep=';', index_col=False)
            self.data_dict['dd-c26'] = pd.read_csv(dataPath + 'pca-chnn-data/pca-chnn-data26.csv', sep=';', index_col=False)
            self.data_dict['dd-c16'] = pd.read_csv(dataPath + 'pca-chnn-data/pca-chnn-data16.csv', sep=';', index_col=False)

            self.voice_dict['vv-pca'] = pd.read_csv(dataPath + 'voice_pca.csv', sep=';', index_col=False)
            self.voice_dict['vv-ses'] = pd.read_csv(dataPath + 'voice_session.csv', sep=';', index_col=False)
            self.voice_dict['vv-a04'] = pd.read_csv(dataPath + 'pca-avg-voice/pca-avg-voice4.csv', sep=';', index_col=False)
            self.voice_dict['vv-a06'] = pd.read_csv(dataPath + 'pca-avg-voice/pca-avg-voice6.csv', sep=';', index_col=False)
            self.voice_dict['vv-a36'] = pd.read_csv(dataPath + 'pca-avg-voice/pca-avg-voice36.csv', sep=';', index_col=False)
            self.voice_dict['vv-c08'] = pd.read_csv(dataPath + 'pca-chnn-voice/pca-chnn-voice8.csv', sep=';', index_col=False)
            self.voice_dict['vv-c13'] = pd.read_csv(dataPath + 'pca-chnn-voice/pca-chnn-voice13.csv', sep=';', index_col=False)
            self.voice_dict['vv-c18'] = pd.read_csv(dataPath + 'pca-chnn-voice/pca-chnn-voice18.csv', sep=';', index_col=False)

            self.ds_dict.update(self.data_dict)
            self.ds_dict.update(self.voice_dict)

    def _get_data(self, data_type=None):
        data = self.features

        keys = {}
        dict_key = data_type

        for num in range(0, 2):
            dict_key = random.choice(list(self.ds_dict.keys()))
            if dict_key in keys: break
            keys[dict_key] = True

            ds = self.ds_dict[dict_key]
            data = data.merge(ds.iloc[:, 1:], left_on='SK_ID', right_on='SK_ID', how='inner')

        # -----------------------------------------------------------------------------
        # split
        # -----------------------------------------------------------------------------
        y_data = data['CSI']
        X_data = data.iloc[:, 1:]

        ret = DataXyz(dict_key)
        ret.x = X_data
        ret.y = y_data
        return ret

    def _get_data3(self, xy):
        xy.x.reset_index(drop=True, inplace=True)
        xy.y.reset_index(drop=True, inplace=True)

        ret = DataXyz(xy.code)
        ret.z = pd.concat([xy.y, xy.x['SK_ID']], axis=1)
        ret.x = xy.x.drop(['SK_ID'], axis=1)
        ret.y = xy.y

        return ret

    def get_data(self, data_type=None):
        data_xy = self._get_data(data_type)

        data_xyz = self._get_data3(data_xy)
        return data_xyz

    def train_test(self, data_type=None, test_size=0.20, random_state=None):
        data_xy = self._get_data(data_type)

        x_train = data_xy.x
        y_train = data_xy.y
        x_test = pd.DataFrame(data=None, columns=x_train.columns)
        y_test = pd.Series(data=None, name=data_xy.y.name)

        if test_size > 0:
            x_train, x_test, y_train, y_test = train_test_split(data_xy.x, data_xy.y, stratify=data_xy.y,
                                                                random_state=random_state, test_size=test_size)

            ros = RandomUnderSampler(random_state=random_state)
            # ros = RandomOverSampler(random_state=random_state)
            # ros = SMOTE()
            ros.fit(x_train, y_train)
            x_train2, y_train2 = ros.fit_resample(x_train, y_train)

            x_train = pd.DataFrame(x_train2, columns=x_train.columns)
            y_train = pd.Series(y_train2, name=y_train.name)

        train = DataXyz(data_xy.code, x_train, y_train)
        train = self._get_data3(train)

        test = DataXyz(data_xy.code, x_test, y_test)
        test = self._get_data3(test)

        return (train, test)

#-----------------------------------------------------------------------------
# return (DataXyz('a'), DataXyz('b'))
# -----------------------------------------------------------------------------
def save_result(folder_out, alg_name, file_prefix, z_data, score, a_prob):
    today = str(date.today())
    path = os.path.join(folder_out, today + '-' + alg_name)

    if not os.path.exists(path):
        os.makedirs(path)

    file_name = file_prefix + '-' + str(int(100000 * score))
    file_name = os.path.join(path, file_name + '.csv')
    print(f'Save file: {file_name}')

    temp = pd.concat([z_data, pd.Series(a_prob, name='PROB')], axis=1)
    temp.to_csv(file_name, sep=';', index=False)


def encoder(X_data, X_test, X_submit):
    cell_id_cols = []
    for col in X_data.columns:
        if (col.startswith('DATA_CID')):
            cell_id_cols.append(col)

        if (col.startswith('VOICE_CID')):
            cell_id_cols.append(col)

    hashEnc = ce.HashingEncoder(n_components=10, cols=cell_id_cols)
    hashEnc.fit(X_data)

    X_data = hashEnc.transform(X_data)
    X_test = hashEnc.transform(X_test)

    if X_submit is not None:
        X_submit = hashEnc.transform(X_submit)
        
    return (X_data, X_test, X_submit)

# -----------------------------------------------------------------------------
#
#-----------------------------------------------------------------------------
if 1 == 11:
    train_ds = RawData('/mnt/raid1/data/condition-csi-out/train/')
    test_ds = RawData('/mnt/raid1/data/condition-csi-out/test/')

    train0 = train_ds.get_data(data_type='None')
    (train1, test1) = train_ds.train_test(data_type='dd-ses', test_size=0.2)

    code = train0.code
    train0_x = train0.x
    train0_y = train0.y
    train0_z = train0.z

    train1_x = train1.x
    train1_y = train1.y
    train1_z = train1.z

    test1_x = test1.x
    test1_y = test1.y
    test1_z = test1.z

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------

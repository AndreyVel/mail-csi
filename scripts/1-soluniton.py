import lightgbm

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import category_encoders as ce

from datetime import date
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from sklearn.model_selection import KFold
from sklearn.model_selection import GroupKFold
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import StratifiedKFold

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------
folder_out = '/mnt/raid1/data/condition-csi-out/'
train_ds = RawData(folder_out + 'train/')
test_ds = RawData(folder_out + 'test/')

parameters = {
    'application': 'binary',
    'objective': 'binary',
    'metric': 'auc',
    'is_unbalance': 'true',
    'boosting': 'gbdt',
    'num_leaves': 35,
    'feature_fraction': 0.5,
    'bagging_fraction': 0.5,
    'bagging_freq': 20,
    'learning_rate': 0.05,
    'verbose': -1
}

total_num = 1000
total_score = 0
for num in range(0, total_num):
    (train, test) = train_ds.train_test(data_type=None)

    data_type = train.code
    submit = test_ds.get_data(data_type=data_type)

    train_data = lightgbm.Dataset(train.x, label=train.y)
    test_data = lightgbm.Dataset(test.x, label=test.y)
    model = lightgbm.train(parameters, train_data, valid_sets=test_data, early_stopping_rounds=300)

    prob_test = model.predict(test.x)
    score = roc_auc_score(test.y, prob_test)
    total_score += score
    print(f'Analyse {num}/{total_num}, roc_auc_score={score}')

    if score > 0.56:
        prob_test = model.predict(test.x)
        save_result(folder_out, 'lgbm', data_type + '-t', test.z, score, prob_test)

        prob_test = model.predict(submit.x)
        save_result(folder_out, 'lgbm', data_type + '-s', submit.z, score, prob_test)

print(f'===== total_score={total_score/total_num}')

# -----------------------------------------------------------------------------
#  print(z_test.groupby('CSI').CSI.count())
# -----------------------------------------------------------------------------

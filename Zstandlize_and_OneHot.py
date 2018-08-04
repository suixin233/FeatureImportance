import numpy as np
import random
from sklearn import preprocessing
import pandas as pd
import os

def Zstandlize(data):
    scaler = preprocessing.StandardScaler().fit(data)
    mean = scaler.mean_
    std = scaler.scale_
    data_scaled = scaler.transform(data)
    return data_scaled, mean, std

def OneHot(data, label):
    df = pd.DataFrame(data, columns=label)
    data_OneHot = pd.get_dummies(df)
    return

if __name__ == '__main__':
    os.system('cls')

    # 获取数据
    df = pd.read_csv('raw_sample_full.csv', header=0, encoding='utf-8')
    label_1 = list(df.columns.values)[4:]     #要标签取4，不要标签取5
    #无用代码
    # tmp = np.loadtxt(open('test_sample.csv', encoding='utf8'), dtype=np.str, delimiter=',')
    # total_sample = tmp.shape[0]
    # fieldname = tmp[0, 5:].astype(np.str)
    # data = tmp[1:, 5:].astype(np.float)
    # label = tmp[1:, 4].astype(np.str)
    # row = data.shape[0]
    # col = data.shape[1]

    # Z标准化
    data_scaled, mean, std = Zstandlize(df.drop(['Id','SYSTEM_GLASS_ID','GLASS_ID','PPID','Label'], axis=1))
    data_scaled = pd.DataFrame(data_scaled)

    #标签编码
    le = preprocessing.LabelEncoder()
    label_encoded = pd.DataFrame(le.fit_transform(df['Label'].values))
    convert_list = le.inverse_transform(range(6))
    print(label_encoded)
    print('for 0,1,2,3,4,5 are ' +  ','.join(convert_list))


    #独热编码
    # data_test = {'color': ['green', 'red', 'blue'],
    #         'size': ['M', 'L', 'XL'],
    #         'price': [10.1, 13.5, 15.3],
    #         'classlabel': ['class1', 'class2', 'class1']}
    # df = pd.DataFrame(data_test)
    # data_1 = pd.get_dummies(df)

    # 数据保存
    data_new = pd.concat([label_encoded, data_scaled], axis=1)
    data_save = pd.DataFrame(data=data_new.values, columns=label_1)
    # data_save = pd.DataFrame(data=data_scaled.values, columns=label_1)
    data_save.to_csv('E:/sharespace/DataProcess/test_sample_full.csv', index_label='Id')
    print(data_scaled)

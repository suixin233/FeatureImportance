import time
import xgboost as xgb
import pandas as pd
from sklearn.model_selection import train_test_split
import numpy as np
import matplotlib.pyplot as plt

def xgboost_train(file, num_class, num_rounds, early_stopping_rounds):
    # 记录程序运行时间
    start_time = time.time()

    # 读入数据
    # train = pd.read_csv('DigitRecognizer/train.csv')
    # tests = pd.read_csv('DigitRecognizer/test.csv')
    train = pd.read_csv(file)
    label_this = train.columns.values.tolist()

    # 用sklearn.model_selection进行训练数据集划分，这里训练集和交叉验证集比例为8：2，可以自己根据需要设置
    train_xy, val = train_test_split(train, test_size=0.3, random_state=1)

    y = train_xy.Label
    x = train_xy.drop(['Id', 'Label'], axis=1)
    val_y = val.Label
    val_x = val.drop(['Id', 'Label'], axis=1)

    # xgb矩阵赋值
    xgb_val = xgb.DMatrix(val_x, label=val_y)
    xgb_train = xgb.DMatrix(x, label=y)
    # xgb_test = xgb.DMatrix(tests)
    # 先用原样本试一试
    xgb_test = xgb.DMatrix(train.drop(['Id', 'Label'], axis=1))

    params = {
        'booster': 'gbtree',
        'objective': 'multi:softmax',  # 多分类问题
        'num_class': num_class,  # 类别数，与multisoftmax并用
        'gamma': 0.1,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
        'max_depth': 12,  # 构建树的深度，越大越容易过拟合
        'lambda:': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
        'subsample': 0.7,  # 随机采样训练样本
        'colsample_bytree': 0.7,  # 生成树时进行的列采样
        'mid_child_weight': 3,
        # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
        # 假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
        # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
        'silent': 0,  # 如同学习率
        'eta': 0.007,
        'seed': 1000,
        'nthread': 0,  # cpu 线程数
        # 'eval_metric': 'auc'
    }

    plst = list(params.items())
    # num_rounds = 5000  # 迭代次数
    watchlist = [(xgb_train, 'train'), (xgb_val, 'val')]

    # 训练模型并保存
    # early_stopping_rounds 当设置的迭代次数较大时，early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    save_location = './model/xgb.model'
    model = xgb.train(plst, xgb_train, num_rounds, watchlist, early_stopping_rounds=early_stopping_rounds)
    model.save_model(save_location)  # 用于存储训练出的模型



    print('best best_ntree_limit', model.best_ntree_limit)

    preds = model.predict(xgb_test, ntree_limit=model.best_ntree_limit)
    # np.savetxt('xgb_submission.csv', np.c_[range(1,len(tests)+1), preds], delimiter=',', header='ImageId, Label', comments='', fmt='%d')
    np.savetxt('./model/xgb_submission.csv', np.c_[range(1, len(train) + 1), preds], delimiter=',',
               header='ImageId, Label', comments='', fmt='%d')

    # 输出运行时长
    cost_time = time.time() - start_time
    print('xgboost success!', '\n', 'cost time: ', cost_time, '(s)......')

    # # 画重要性图和树状图
    xgb.plot_importance(model)
    xgb.plot_tree(model)
    plt.show()



if __name__ == '__main__':
    xgboost_train(file='test_sample_full_featurefilted_5k.csv', num_class=6, num_rounds=100, early_stopping_rounds=100)
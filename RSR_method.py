import numpy as np
import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import random


def loadcsv(file, choose_number):
    tmp = np.loadtxt(open(file, encoding='utf8'), dtype=np.str, delimiter=',')
    total_sample = tmp.shape[0]
    slice_sample = random.sample(range(1,total_sample), choose_number)
    label = tmp[0,2:].astype(np.str)
    data = tmp[slice_sample,2:].astype(np.float)
    row = data.shape[0]
    col = data.shape[1]
    return data, label, row, col


def RSRscore(data, row, col, nambda, k_iteration, epsion):
    X = np.mat(data)
    GL = np.mat(np.eye(row))
    GR = np.mat(np.eye(col))
    w = np.ones((col,col))
    k = k_iteration
    while k >= 0:
        gL = np.zeros((1,row))
        gR = np.zeros((1,col))
        tmp = GR.I * (X.T * GL * X)
        w = (tmp + nambda * np.eye(col)).I * tmp
        print(k)
        for i in range(row):
            temp = np.linalg.norm((X[i,:] - X[i,:] * w), ord=2)
            if temp >= epsion:
                gL[0,i] = 0.5 / temp
            else:
                gL[0,i] = 0.5 / epsion
        for i in range(col):
            temp = np.linalg.norm(w[i,:], ord=2)
            if temp >= epsion:
                gR[0,i] = 0.5 / temp
            else:
                gR[0,i] = 0.5 / epsion
        GR = np.mat(np.diag(gR.flat))
        GL = np.mat(np.diag(gL.flat))
        k = k - 1
    score = np.zeros((1,col))
    for i in range(col):
        score[0,i] = np.linalg.norm(w[i,:], ord=2)
    w = (w + np.absolute(w)) / 2
    errorM = X - X * w
    error = np.zeros((1,row))
    for i in range(row):
        error[0,i] = np.linalg.norm(errorM[i,:], ord=2)
    return w, score, error



if __name__ == '__main__':
    os.system('cls')
    [data, label, row, col] = loadcsv('test_sample_full.csv', 5000)
    print(row,col)
    [w, score, error] = RSRscore(data, row, col, 50, 1000, 1e-7)
    for i in range(col):
        if score[0,i] >= 0.01:
            print(str(i) + ' ' + str(label[i]) + ' ' + str(score[0,i]) + ' YES')
        else:
            print(str(i) + ' ' + str(label[i]) + ' ' + str(score[0,i]) + ' NO')

    # 保存数据
    df = pd.DataFrame(w, columns = range(col))
    df.to_csv("RSR_data.csv", encoding="utf-8")
    np.savetxt("RSR_score.txt", score)
    np.savetxt("RSR_error.txt", error)

    # 画图
    plt.figure(1)
    sns.heatmap(df, vmin=-1, vmax=1)
    #sns.heatmap(df, annot=True, vmin=-1, vmax=1)

    plt.figure(2)
    bar_width = 0.3
    plt.bar(np.arange(col), score.tolist()[0], width=0.3, color='y')

    plt.figure(3)
    plt.plot(np.arange(row), error.tolist()[0], marker='*')
    plt.show()

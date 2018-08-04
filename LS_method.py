import numpy as np
import os
import copy
import matplotlib.pyplot as plt
import random
import pandas as pd


def loadcsv(file, choose_number, choose_list):
    tmp = np.loadtxt(open(file, encoding='utf8'), dtype=np.str, delimiter=',')
    total_sample = tmp.shape[0]
    slice_sample = random.sample(range(1, total_sample), choose_number)
    CF_index = [i + 2 for i in choose_list]
    tmp = tmp[:,CF_index]
    label_ls = tmp[0,:].astype(np.str)
    tmp = tmp[slice_sample,:]
    data_ls = tmp[:,:].astype(np.float)
    row_ls = data_ls.shape[0]
    col_ls = data_ls.shape[1]
    return data_ls, label_ls, row_ls, col_ls


def LaplacianScore(data, label, row, col, k_neighbour, t_var):
    origin = np.mat(data)
    graph = np.zeros((row, row))
    k = k_neighbour
    t = t_var
    for i in range(row):
        print(i)
        for j in range(row):
            graph[i,j] = np.square(np.linalg.norm(origin[i,:]-origin[j,:],ord=2))
            # print(i, j, graph[i,j])

    temp = copy.copy(graph)
    for i in range(row):
        a = graph[i,:]
        postToZero = sorted(range(row),key=a.__getitem__)
        postToZero = postToZero[0:k+1]
        # print(postToZero)
        for m in postToZero:
            graph[i,m] = 0

    for i in range(row):
        for j in range(row):
            graph[i,j] = temp[i,j] - graph[i,j]

    graph = np.mat(graph)
    for i in range(row):
        for j in range(i+1,row):
            if graph[i,j] != graph[j,i]:
                graph[i,j] = 0
                graph[j,i] = 0

    S = np.mat(np.zeros((row,row)))
    for i in range(row):
        for j in range(row):
            if graph[i,j] != 0:
                S[i,j] = np.exp(-graph[i,j]/t)

    one_1 = np.mat(np.ones((row, 1)))
    D = np.mat(np.diag((S * one_1).flat))
    fr = np.mat(np.zeros((row,col)))
    for i in range(col):
        fr[:,i] = origin[:,i] - float(((origin[:,i].T * D * one_1) / (one_1.T * D * one_1))) * one_1

    L = np.mat(np.zeros((row, row)))
    for i in range(row):
        for j in range(row):
            L[i,j] = D[i,j] - S[i,j]

    Lr = np.mat(np.zeros((1,col)))
    for i in range(col):
        Lr[0,i] = float((fr[:,i].T * L * fr[:,i]) / (fr[:,i].T * D * fr[:,i]))
        if Lr[0,i] == 'nan':
            Lr[0,i] = 1
        # print(str(i) + ' ' + label[i] + ' ' + str(Lr[0,i]))
    return Lr


if __name__ == '__main__':
    os.system('cls')
    choose_list = [0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 38, 39, 40, 41, 43, 45, 46, 47, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 65, 66, 67, 69, 70, 71, 73, 75, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211]
    [data_af_RSR, label_af_RSR, row_af_RSR, col_af_RSR] = loadcsv('test_sample_full.csv', 5000, choose_list)
    Lr = LaplacianScore(data_af_RSR, label_af_RSR, row_af_RSR, col_af_RSR, 15, 1)
    Lr = Lr.tolist()[0]
    print(Lr)
    use_list_count_after_LS = 0
    choose_list_after_LS = []
    for i in range(len(Lr)):
        if Lr[i] <= 0.5:
            print(str(i) + ' ' + str(label_af_RSR[i]) + ' ' + str(Lr[i]) + ' YES')
            choose_list_after_LS.append({'Field': label_af_RSR[i], 'Score': Lr[i]})
            use_list_count_after_LS = use_list_count_after_LS + 1
        else:
            print(str(i) + ' ' + str(label_af_RSR[i]) + ' ' + str(Lr[i]) + ' NO')

    cl = pd.DataFrame(choose_list_after_LS)
    cl.to_csv('./result/choose_feature.csv', index=False)

    #保存数据
    np.savetxt("LS_score.txt", Lr)
    #画图
    index = np.arange(col_af_RSR)
    bar_width = 0.3
    plt.bar(index, Lr, width=0.3, color='y')
    plt.show()
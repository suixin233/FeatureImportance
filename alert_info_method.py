import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import sklearn.cluster as cl
from sklearn.externals import joblib
from sklearn import metrics
import math
import datetime
import pymysql
import sys
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
import random

def loadcsv(file):
    tmp = np.loadtxt(open(file, encoding='utf8'), dtype=np.str, delimiter=',')
    total_sample = tmp.shape[0]
    label = tmp[0,:].astype(np.str)
    all_data = tmp[1:,:].astype(np.str)
    run_data = tmp[1:,[3,5]].astype(np.float)
    # 0-1标准化
    run_data_standard = run_data.copy()
    run_data_standard[:, 0] = (run_data_standard[:, 0] - min(run_data_standard[:, 0])) / max(run_data_standard[:, 0] - min(run_data_standard[:, 0]))
    run_data_standard[:, 1] = (run_data_standard[:, 1] - min(run_data_standard[:, 1])) / max(run_data_standard[:, 1] - min(run_data_standard[:, 1]))
    # print(run_data)
    return all_data, run_data, run_data_standard, label


def alert_info_method(all_data, run_data_standard, label):
    # k-means聚类
    estimator = cl.KMeans(n_clusters=cluster_number,
                          init='k-means++',
                          n_init=40,
                          max_iter=5000,
                          tol=1e-8,
                          precompute_distances='auto',
                          verbose=0,
                          random_state=None,
                          copy_x=True,
                          n_jobs=-1,
                          algorithm='auto')
    '''  n_clusters:簇的个数，即你想聚成几类
        init: 初始簇中心的获取方法
        n_init: 获取初始簇中心的更迭次数，为了弥补初始质心的影响，算法默认会初始10个质心，实现算法，然后返回最好的结果。
        max_iter: 最大迭代次数（因为kmeans算法的实现需要迭代）
        tol: 容忍度，即kmeans运行准则收敛的条件
        precompute_distances：是否需要提前计算距离，这个参数会在空间和时间之间做权衡，如果是True 会把整个距离矩阵都放到内存中，auto 会默认在数据样本大于featurs*samples 的数量大于12e6 的时候False,False 时核心实现的方法是利用Cpython 来实现的
        verbose: 冗长模式（不太懂是啥意思，反正一般不去改默认值）
        random_state: 随机生成簇中心的状态条件。
        copy_x: 对是否修改数据的一个标记，如果True，即复制了就不会修改数据。bool 在scikit-learn 很多接口中都会有这个参数的，就是是否对输入数据继续copy 操作，以便不修改用户的输入数据。这个要理解Python 的内存机制才会比较清楚。
        n_jobs: 并行设置
        algorithm: kmeans的实现算法，有：’auto’, ‘full’, ‘elkan’, 其中 ‘full’表示用EM方式实现
    '''

    result = estimator.fit_predict(run_data_standard)  #获取聚类标签
    centroids = estimator.cluster_centers_    #获取聚类中心
    inertia = estimator.inertia_              #获取聚类准则的总和
    ch_sorce = metrics.calinski_harabaz_score(run_data_standard, result)

    print('cluster complete')

    # 注释语句用来存储你的模型
    # 模型载入保存
    joblib.dump(estimator, 'alert_cluster.pkl')
    # km = joblib.load('alert_cluster.pkl')
    # result = km.labels_.tolist()

    print('model save as alert_cluster.pkl')
    print('中心点 ', end='')
    print(centroids)
    print('总和 ', end='')
    print(inertia)
    print("Calinski-Harabasz Score" + str(ch_sorce))
    return result, centroids, inertia, run_data_standard, ch_sorce


def alert_info_method_SpectralClustering(run_data_standard, k, gamma):
    result = cl.SpectralClustering(n_clusters=k, gamma=gamma).fit_predict(run_data_standard)
    ch_score = metrics.calinski_harabaz_score(run_data_standard, result)
    print("Calinski-Harabasz Score with gamma=", gamma, "n_clusters=", k, "score:", ch_score)
    return result, ch_score


def cluster_count_and_data_save(cluster_info_save_path, all_data, run_data, label, result, run_data_standard, Master_ID, Generate_Time):
    # 保存聚类数据
    cluster_info = pd.DataFrame(data=result.reshape(result.size, 1), columns=['cluster'])
    run_data_standard_info = pd.DataFrame(data=run_data_standard, columns=['code_standard', 'time_standard'])
    cluster_data = pd.concat([run_data_standard_info, cluster_info], axis=1)
    all_data_info = pd.DataFrame(data=all_data, columns=label)
    data_save = pd.concat([cluster_data, all_data_info], axis=1)
    data_save.to_excel(cluster_info_save_path, index=False)
    print('cluster information save in ' + cluster_info_save_path)

    # 聚类类别统计
    result_list = result.tolist()
    data = []
    list_piece = []
    for i in range(np.size(run_data, 0)):
        list_piece = list(all_data[i, :])
        list_piece.append(result_list[i])
        data.append(list_piece)

    label = list(label)
    label.append('cluster')
    frame = pd.DataFrame(data, columns=label)
    cluster_number_count = frame['cluster'].value_counts()
    distribution = list(cluster_number_count.sort_index())
    distribution = [math.log(i) - 5 for i in distribution]
    distribution = [i / sum(distribution) for i in distribution]
    print(cluster_number_count)
    print('统计完毕')

    # 画图
    mark = ['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o']
    color = ['black', 'red', 'chocolate', 'gold', 'chartreuse', 'palegreen', 'cyan', 'dodgerblue', 'violet', 'deeppink']
    j = 0
    plot_list = []
    machine_list = ['AOI', 'CLN', 'COA', 'DEV', 'DHC', 'DUV', 'EXP', 'OVN', 'PHC', 'SMA', 'LOD', 'UNL', 'CNV']
    machine_prob = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    count = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for i in range(cluster_number):
        for j in range(len(result_list)):
            if result_list[j] == i:
                plot_list.append(run_data_standard[j, :])
                multi = 0
                weight = 0
                if all_data[j, 2] == 'L':
                    weight = 1
                elif all_data[j, 2] == 'H':
                    weight = 2
                number = 0
                eqp_id = all_data[j, 1]
                index = machine_list.index(eqp_id[4:7])
                if index + 1:
                    count[index] = count[index] + weight
        count = [i / sum(count) for i in count]
        machine_prob = [a + b for a, b in zip(machine_prob, list(np.array(count) * distribution[i]))]
        plot_list = pd.DataFrame(data=plot_list, columns=['code', 'time'])
        # 画图
        # plt.figure(i)
        plt.scatter(plot_list['time'], plot_list['code'], color=color[i], marker=mark[i], s=5)
        plot_list = []
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.xlabel("Alert Time", fontsize=24)
    plt.ylabel("Alert Code", fontsize=24)
    plt.show()

    # 存储重要性数据到文件
    machine_importance = []
    for i in range(len(machine_prob)):
        machine_importance.append({'Machine': machine_list[i], 'Importance': machine_prob[i]})
    machine_importance = pd.DataFrame(machine_importance)
    machine_importance_sorted = machine_importance.sort_values(
        by='Importance', ascending=False).reset_index(drop=True)
    machine_importance.to_csv('./result/alert_machine_imporatance.csv', index=False)
    machine_importance_sorted.to_csv('./result/alert_machine_importance_sorted.csv', index=False)
    print(machine_importance_sorted)

    # 保存alert产生的可疑机台概率到machine_importance表
    try:
        conn = pymysql.connect(host="202.120.8.1", port=3306, user='ren', passwd='123456', db='fles')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    strsql = 'insert into machine_importance value (NULL, ' + Master_ID + ', "alert", "' + Generate_Time + '", "' + '","'.join(
        str(i) for i in machine_prob[0:10]) + '", "")'
    cur.execute(strsql)
    machine_importance_ID = int(cur.lastrowid)
    conn.commit()
    print('alert values upload to table machine_importance')

    cur.close()
    conn.close()
    return machine_importance_ID

if __name__ == '__main__':
    os.system('cls')
    Master_ID = '20180127003'
    Generate_Time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_path = 'E:/sharespace/DataProcess/alert_cluster_info.xlsx'

    #ALERT_COMMENT中如果有#号加载会出错
    [all_data, run_data, run_data_standard, label] = loadcsv('raw_alert_info.csv')

    """
    # CH准则训练
    ch_score_list = []
    x = range(2,100,5)
    for i in x:
        cluster_number = i
        print('running cluster number of ' + str(i))
        [result, centroids, inertia, run_data_standard, ch_score]  = alert_info_method(all_data, run_data_standard, label)
        ch_score_list.append(ch_score)
        with open("ch_score.txt", "a") as f:
            f.write(str(i) + ' classes with ch score as: ' + str(ch_score) + '\n')
    plt.plot(x, ch_score_list, marker='o', mec='r', mfc='w', label=u'聚类合适程度曲线图')
    plt.legend()
    plt.margins(0)
    plt.subplots_adjust(bottom=0.15)
    plt.xlabel(u"聚类类团数")  # X轴标签
    plt.ylabel("CH评分")  # Y轴标签
    plt.title("选取合适类团数")  # 标题
    plt.show()

    ## SpectralClustering
    # slice_index = random.sample(range(run_data_standard.shape[0]), 5000)
    # run_data_random = run_data_standard[slice_index, :]
    # for index, gamma in enumerate((0.01, 0.1, 1, 10, 100)):
    #     for index, k in enumerate((3, 4, 5, 6, 7, 8 ,9 ,10)):
    #         [result, ch_score] = alert_info_method_SpectralClustering(run_data_random, k, gamma)
    #         cluster_number = k
    #         cluster_count_and_data_save(save_path, all_data, run_data, label, result, run_data_random, Master_ID, Generate_Time)
    """

    # 画图
    cluster_number = 10
    [result, centroids, inertia, run_data_standard, ch_score] = alert_info_method(all_data, run_data_standard, label)
    cluster_count_and_data_save(save_path, all_data, run_data, label, result, run_data_standard, Master_ID, Generate_Time)




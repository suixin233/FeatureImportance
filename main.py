import LS_method as LS
import RSR_method as RSR
import preprocess as PRE
import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import DS_fusion_method
import datetime
import pymysql
import sys
import alert_info_method
import DigitRecognizer

def get_filter_data(file, filter_list):
    tmp = np.loadtxt(open(file, encoding='utf8'), dtype=np.str, delimiter=',')
    label_all = tmp[0,2:].astype(np.str)
    data_all = tmp[1:,2:].astype(np.float)
    rest = tmp[1:,0:2].astype(np.str)

    # 输出对应的数据集
    label_list = label_all.tolist()
    list_index = []
    index = -1
    # label_list = list(label)
    for i in filter_list:
        index = label_list.index(i['Field'])
        list_index.append(index)

    total_list = range(215)
    delete_list = list(set(total_list) - set(list_index))
    choose_feature_and_data = np.delete(data_all, delete_list, axis=1)
    choose_feature_label = [label_list[i] for i in list_index]
    data_filter = pd.DataFrame(data=choose_feature_and_data, columns=choose_feature_label)
    data_rest = pd.DataFrame(data=rest, columns=['Id', 'Label'])
    data_save = pd.concat([data_rest, data_filter], axis=1)
    return data_save

if __name__ == '__main__':
    os.system('cls')
    sample_size = 10000
    RSR_threshold = 1e-3
    LS_threshold = 0.3
    Master_ID = '20180131001'
    Generate_Time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    filted_file_path = 'E:/sharespace/DataProcess/test_sample_full_featurefilted.csv'
    raw_alert_path = 'E:/sharespace/DataProcess/raw_alert_info.csv'
    cluster_info_save_path = 'E:/sharespace/DataProcess/alert_cluster_info.xlsx'
    processdata_feature_importance_ID = -1
    processdata_machine_importance_ID = -1
    alert_machine_importance_ID = -1

    want_dataframe = 0
    want_train = 0
    want_alert = 0
    want_fusion = 1

    if want_dataframe:
        ##########################################################################
        # RSR
        [data, label, row, col] = RSR.loadcsv('test_sample_full.csv', sample_size)
        print(row, col)
        label_list = label.tolist()
        [w, score, error] = RSR.RSRscore(data, row, col, 100, 1000, 1e-5)
        choose_list = []
        use_list_count_after_RSR = 0
        for i in range(col):
            if score[0, i] >= RSR_threshold:
                print(str(i) + ' ' + str(label[i]) + ' ' + str(score[0, i]) + ' YES')
                choose_list.append(i)
                use_list_count_after_RSR = use_list_count_after_RSR + 1
            else:
                print(str(i) + ' ' + str(label[i]) + ' ' + str(score[0, i]) + ' NO')

        #上传RSR结果到data_frame表
        try:
            conn = pymysql.connect(host="202.120.8.1", port=3306, user='ren', passwd='123456', db='fles')
            cur = conn.cursor()
        except pymysql.Error:
            print("数据库连接异常")
            sys.exit(-1)

        strsql = 'insert into data_frame value (NULL, ' + Master_ID + ', "RSR", "' + Generate_Time + '", "' + '","'.join(str(i) for i in score.tolist()[0]) + '")'
        cur.execute(strsql)
        conn.commit()
        print('RSR values upload to table data_frame')

        # # 画图
        # plt.figure(1)
        # df = pd.DataFrame(w, columns=range(col))
        # sns.heatmap(df, vmin=-1, vmax=1)
        # # sns.heatmap(df, annot=True, vmin=-1, vmax=1)
        #
        # plt.figure(2)
        # bar_width = 0.3
        # plt.bar(np.arange(col), score.tolist()[0], width=0.3, color='y')
        #
        # plt.figure(3)
        # plt.plot(np.arange(row), error.tolist()[0], marker='*')

        ##########################################################################
        # LS
        [data_af_RSR, label_af_RSR, row_af_RSR, col_af_RSR] = LS.loadcsv('test_sample_full.csv', sample_size, choose_list)
        Lr = LS.LaplacianScore(data_af_RSR, label_af_RSR, row_af_RSR, col_af_RSR, 15, 1)
        Lr = Lr.tolist()[0]
        print(Lr)
        use_list_count_after_LS = 0
        choose_list_after_LS = []
        judge = np.zeros((1,215))
        for i in range(len(Lr)):
            if Lr[i] <= LS_threshold:
                print(str(i) + ' ' + str(label_af_RSR[i]) + ' ' + str(Lr[i]) + ' YES')
                choose_list_after_LS.append({'Field': label_af_RSR[i], 'Score': Lr[i]})
                judge[0,label_list.index(label_af_RSR[i])] = 1
                use_list_count_after_LS = use_list_count_after_LS + 1
            else:
                print(str(i) + ' ' + str(label_af_RSR[i]) + ' ' + str(Lr[i]) + ' NO')

        cl = pd.DataFrame(choose_list_after_LS)
        cl.to_csv('./result/choose_feature.csv', index=False)
        print('after RSR ' + str(use_list_count_after_RSR) + ' features were used')
        print('after LS ' + str(use_list_count_after_LS) + ' features were used')

        #上传LS结果到data_frame表
        strsql = 'insert into data_frame (MasterID, Method, Generate_Time, `' + '`,`'.join(label_af_RSR) + '`) value (' + Master_ID + ', "LS", "' + Generate_Time + '", "' + '","'.join(str(i) for i in Lr) + '")'
        cur.execute(strsql)
        conn.commit()
        print('LS values upload to table data_frame')

        # 上传Judge结果到data_frame表
        strsql = 'insert into data_frame value (NULL, ' + Master_ID + ', "Judge", "' + Generate_Time + '", "' + '","'.join(str(i) for i in judge.tolist()[0]) + '")'
        cur.execute(strsql)
        conn.commit()
        print('Judge upload to table data_frame')

        cur.close()
        conn.close()

        # # 画图
        # index = np.arange(col_af_RSR)
        # bar_width = 0.3
        # plt.figure(4)
        # plt.bar(index, Lr, width=0.3, color='y')
        # plt.show()

        #############################################################################
        #输出对应的数据集

        data_save = get_filter_data('test_sample_full.csv', choose_list_after_LS)
        data_save.to_csv('E:/sharespace/DataProcess/test_sample_full_featurefilted.csv', index=False)
        print('filter_data in ' + filted_file_path)

    if want_train:
        [ft_importance, feat_importances_sorted, machine_importance, machine_importances_sorted] = DigitRecognizer.xgboost_train(
            filted_file_path, 6, 5000, 100)
        [feature_importance_ID, machine_importance_ID] = DigitRecognizer.result_upload_and_data_save(ft_importance, machine_importance, Master_ID, Generate_Time)
        processdata_feature_importance_ID = feature_importance_ID
        processdata_machine_importance_ID = machine_importance_ID
        print('processdata_feature_importance_ID is ' + str(processdata_feature_importance_ID))
        print('processdata_machine_importance_ID is ' + str(processdata_machine_importance_ID))

    if want_alert:
        [all_data, run_data, label] = alert_info_method.loadcsv(raw_alert_path)
        [result, centroids, inertia, run_data_standard] = alert_info_method.alert_info_method(all_data, run_data, label)
        machine_importance_ID = alert_info_method.cluster_count_and_data_save(cluster_info_save_path, all_data, run_data, label, result, run_data_standard, Master_ID, Generate_Time)
        alert_machine_importance_ID = machine_importance_ID
        print('alert_machine_importance_ID is ' + str(alert_machine_importance_ID))

    if want_fusion:
        machine_list = ['AOI', 'CLN', 'COA', 'DEV', 'DHC', 'DUV', 'EXP', 'OVN', 'PHC', 'SMA']
        processdata_importance_ID = processdata_machine_importance_ID
        alert_importance_ID = alert_machine_importance_ID

        #获取processdata和警报的可疑机台概率
        try:
            conn = pymysql.connect(host="202.120.8.1", port=3306, user='ren', passwd='123456', db='fles')
            cur = conn.cursor()
        except pymysql.Error:
            print("数据库连接异常")
            sys.exit(-1)

        strsql = 'select ' + ','.join(machine_list) + ' from machine_importance where Id = ' + str(processdata_importance_ID)
        cur.execute(strsql)
        cur.fetchone()
        # process_data_generate = [float(i) for i in list(cur._rows[0])]

        strsql = 'select ' + ','.join(machine_list) + ' from machine_importance where Id = ' + str(alert_importance_ID)
        cur.execute(strsql)
        # alert_info_generate = [float(i) for i in list(cur._rows[0])]

        # adjust before
        # process_data_generate = [0.07397643357560045, 0.1563596110838198, 0.022575993707852068,
        #                          0.13525999467709712, 0.013867353788993241, 0.010835152874566191,
        #                          0.5152172790432545, 0.06874356981410887, 0.0008964192223000502,
        #                          0.0006117366036966368]
        # alert_info_generate = [0.314946303, 0.005148367, 0.302361762, 0.028331937, 0.03583703,
        #                        0.163092409, 0.075498743, 0.008747403, 0.007902716, 0.002539015]

        # adjust after
        process_data_generate = [0.084356007, 0.178299789, 0.025743771, 0.01581276, 0.01581276 ,
                                 0.012355322, 0.58751012, 0.078389874, 0.001021723, 0.000697873]
        alert_info_generate = [0.330856909, 0.005408074, 0.317637172, 0.037647467, 0.037647467,
                               0.171331323, 0.07931317, 0.009188894, 0.008302255, 0.002667269]

        importance_sample = []
        importance_sample.append(process_data_generate)
        importance_sample.append(alert_info_generate)
        importance_sample = np.array(importance_sample)

        #开始合成
        machine_list_new = machine_list.copy()
        fusion = DS_fusion_method.use_DS_method_of_sun(importance_sample, machine_list_new)

        #上传信息融合结果到machine_importance
        fusion_info = fusion[-1:].values
        strsql = 'insert into fusion_importance(MasterID, ProcessData_Source_ID, Alert_Source_ID, Generate_Time, `' + '`,`'.join(machine_list_new) + \
                 '`) value ("' + Master_ID + '", ' + str(processdata_importance_ID) + ', ' + str(alert_importance_ID) + ', "' + Generate_Time + '", "' + '","'.join(
            str(i) for i in fusion_info[0].tolist()) + '")'
        cur.execute(strsql)
        fusion_ID = int(cur.lastrowid)
        conn.commit()
        print('fusion values upload to table fusion_importance')
        print('fusion_ID is ' + str(fusion_ID))

        cur.close()
        conn.close()





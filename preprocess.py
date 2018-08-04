import pymysql
import sys
import random
import numpy as np
import csv
import pandas as pd


def data_preprocessing(old_table_name, new_table_name):
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur_seq = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    #判断table是否存在
    table_field_list = process_data_fields(old_table_name)
    if table_field_list == -1:
        return 'table not exist'

    #获取SEQ最大值
    cur_seq.execute('select max(SEQ) from ' + old_table_name)
    tmp = list(cur_seq.fetchall())
    max_seq = int(str(tmp[0])[1:-2])

    count = 0
    using_id = -1
    cur = conn.cursor()
    cur_new = conn.cursor()
    cur_insert = conn.cursor()
    cur.execute('select  Id, EQP_ID, SYSTEM_GLASS_ID, GLASS_ID, DATA_TYPE, MEASURE_DATE, PPID, IS_PROCESSED, SEQ from ' + old_table_name + ' where SEQ = 6 order by Id')
    for r in cur.fetchall():
        print('processing ID = ' + str(r[0]))
        print('processing SEQ = ' + str(r[8]))
        print('processing GLASS_ID = ' + str(r[3]))
        using_seq = 6
        using_id = r[0]
        list1 = []
        exit_flag = False
        while using_seq <= len(table_field_list) + 5 :
            cur_new.execute('select SEQ, PROCESS_DATA_VALUE from ' + old_table_name + ' where Id = ' + str(using_id))
            print(cur_new._rows)
            for abc in cur_new._rows:
                if len(list(abc)) > 0 and int(abc[0]) == using_seq:
                    list1.append(abc[1])
                    using_seq = using_seq + 1
                    using_id = using_id + 1
                else:
                    exit_flag = True
                    break
            if exit_flag:
                break
        if exit_flag:
            continue
        if len(list1) == len(table_field_list):
            try:
                strsql = 'insert into ' + new_table_name + ' (MasterID, EQP_ID, SYSTEM_GLASS_ID, GLASS_ID, DATA_TYPE, MEASURE_DATE, PPID, IS_PROCESSED, '
                strsql += '`' + '`, `'.join(table_field_list) + '`) '
                strsql += 'value (' + str(r[0]) + ',"' + '","'.join(r[1:5]) + '","' + str(r[5]) + '","' + '","'.join(r[6:-1]) + '","' + '","'.join(list1) + '")'
                cur_insert.execute(strsql)
                conn.commit()
                print(str(using_id) + 'success')
                print()
                count = count + 1
            except (IOError, ConnectionError, pymysql.Error):
                print('输入数据库异常')
    cur_seq.close()
    cur.close()
    cur_new.close()
    cur_insert.close()
    conn.close()
    return count


def time_processing():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    try:
        cur.execute('update bl1_aoi_01_merge  set `BL1-AOI-01-Carry-time-YYYY` = (`BL1-AOI-01-Carry-out-time-YYYY` - `BL1-AOI-01-Carry-in-time-YYYY`)/1000')
        cur.execute('update bl1_cln_01_merge  set `BL1-CLN-01-Glass-Time` = (`BL1-CLN-01-Glass-Unloading-Time` - `BL1-CLN-01-Glass-Loading-Time`)/1000')
        cur.execute('update bl1_dhc_01_merge  set `BL1-DHC-01-HP-TIME` = (`BL1-DHC-01-HP-END-TIME` - `BL1-DHC-01-HP-START-TIME`)/1000')
        cur.execute('update bl1_dhc_01_merge  set `BL1-DHC-01-CP-TIME` = (`BL1-DHC-01-CP-END-TIME` - `BL1-DHC-01-CP-START-TIME`)/1000')
        cur.execute('update bl1_ovn_01_merge  set `BL1-OVN-01-LD-TIME-1` = (`BL1-OVN-01-ULD-OUT-TIME-1` - `BL1-OVN-01-LD-IN-TIME-1`)/1000')
        cur.execute('update bl1_phc_01_merge  set `BL1-PHC-01-HP-TIME` = (`BL1-PHC-01-HP-END-TIME` - `BL1-PHC-01-HP-START-TIME`)/1000')
        cur.execute('update bl1_phc_01_merge  set `BL1-PHC-01-CP-TIME` = (`BL1-PHC-01-CP-END-TIME` - `BL1-PHC-01-CP-START-TIME`)/1000')
        cur.execute('update bl1_sma_01_merge  set `BL1-SMA-01-Inspection-Time-1` = (`BL1-SMA-01-Inspection-end-Time-1` - `BL1-SMA-01-Inspection-start-Time-1`)/1000')
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except pymysql.Error:
        print(pymysql.Error.args)
        return -1


def process_data_merge():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    a = merge_data_fields('bl1_aoi_01_merge')
    cl = merge_data_fields('bl1_cln_01_merge')
    co = merge_data_fields('bl1_coa_01_merge')
    de = merge_data_fields('bl1_dev_01_merge')
    dh = merge_data_fields('bl1_dhc_01_merge')
    du = merge_data_fields('bl1_duv_01_merge')
    e = merge_data_fields('bl1_exp_01_merge')
    o = merge_data_fields('bl1_ovn_01_merge')
    p = merge_data_fields('bl1_phc_01_merge')
    s = merge_data_fields('bl1_sma_01_merge')
    try:
        strsql = 'insert into bl1_01_overall_merge(SYSTEM_GLASS_ID,GLASS_ID,PPID,`'+ '`,`'.join(a) + '`,`' + '`,`'.join(cl) + '`,`' + '`,`'.join(co) +\
                 '`,`' + '`,`'.join(de) + '`,`' + '`,`'.join(dh) + '`,`' + '`,`'.join(du) + '`,`' + '`,`'.join(e) + '`,`' + '`,`'.join(o) + '`,`' + '`,`'.join(p) + '`,`' + '`,`'.join(s) + '`) ' \
                 'select a.SYSTEM_GLASS_ID,a.GLASS_ID,a.PPID,a.`' + '`,a.`'.join(a) + '`,cl.`' + '`,cl.`'.join(cl) + '`,co.`' + '`,co.`'.join(co) + \
                 '`,de.`' + '`,de.`'.join(de) + '`,dh.`' + '`,dh.`'.join(dh) + '`,du.`' + '`,du.`'.join(du) + '`,e.`' + '`,e.`'.join(e) + \
                 '`,o.`' + '`,o.`'.join(o) + '`,p.`' + '`,p.`'.join(p) + '`,s.`' + '`,s.`'.join(s) + '` ' \
                 'from ((((((((bl1_aoi_01_merge as a inner join bl1_cln_01_merge as cl on a.GLASS_ID = cl.GLASS_ID) inner join bl1_coa_01_merge as co on a.GLASS_ID = co.GLASS_ID) ' \
                 'inner join bl1_dev_01_merge as de on a.GLASS_ID = de.GLASS_ID) left join bl1_dhc_01_merge as dh on a.GLASS_ID = dh.GLASS_ID) left join bl1_duv_01_merge as du on a.GLASS_ID = du.GLASS_ID) ' \
                 'inner join bl1_exp_01_merge as e on a.GLASS_ID = e.GLASS_ID) inner join bl1_ovn_01_merge as o on a.GLASS_ID = o.GLASS_ID) left join bl1_phc_01_merge as p on a.GLASS_ID = p.GLASS_ID) ' \
                 'left join bl1_sma_01_merge as s on a.GLASS_ID = s.GLASS_ID'
        cur.execute(strsql)
        conn.commit()
        cur.close()
        conn.close()
        return 1
    except pymysql.Error:
        print(pymysql.Error.args)
        return -1


def process_data_simple_notnull():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    #取同一GLASS_ID项的第一个，并记录有多少个这个GLASS_ID
    cur.execute('insert into bl1_01_overall_simple select *, count(GLASS_ID) from bl1_01_overall_merge group by GLASS_ID order by Id')
    conn.commit()

    #把null转化为0
    list_table = ('bl1_aoi_01_merge', 'bl1_cln_01_merge', 'bl1_coa_01_merge', 'bl1_dev_01_merge',  'bl1_dhc_01_merge', 'bl1_duv_01_merge', 'bl1_exp_01_merge', 'bl1_ovn_01_merge', 'bl1_phc_01_merge', 'bl1_sma_01_merge')
    for l in list_table:
        fields = merge_data_fields(l)
        for field in fields:
            cur.execute('update bl1_01_overall_simple set `' + field + '` = ifnull(`' + field + '`, 0)')
            conn.commit()

    #对count取以2为底的对数+1
    cur.execute('update bl1_01_overall_simple set count = log(2,count) + 1')
    conn.commit()

    cur.close()
    conn.close()


def process_data_scale_ajust(table_name, choose_number):
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
        cur_ID = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)
    cur_ID.execute('select ID from ' + table_name)
    ID_list = cur_ID.fetchall()
    slice_sample = random.sample(ID_list, choose_number)
    for sample_ID in slice_sample:
        cur.execute('select * from ' + table_name + ' where ID = ' + str(sample_ID[0]))
        data = cur.fetchone()
        label = cur.description
        for i in range(4,219):
            a = float(data[i])
            b = label[i][0]
            if (a >= 0.1 and a < 1) or (a >= -1 and a < -0.1):
                cur.execute('update ' + table_name + ' set `' + b + '` = `' + b + '` * 10')
                conn.commit()
                print(str(i) + ' success')
            elif (a >= 0.01 and a < 0.1) or (a >= -0.1 and a < -0.01):
                cur.execute('update ' + table_name + ' set `' + b + '` = `' + b + '` * 100')
                conn.commit()
                print(str(i) + ' success')
            elif (a > 0 and a < 0.01) or (a >= -0.01 and a < 0):
                cur.execute('update ' + table_name + ' set `' + b + '` = `' + b + '` * 1000')
                conn.commit()
                print(str(i) + ' success')
    cur.close()
    conn.close()


def process_data_fields(table_name):
    if table_name == 'h_spc_pdrd_bl1_aoi_01':
        list1 = ('BL1-AOI-01-Carry-in-time-YYYY', 'BL1-AOI-01-Carry-out-time-YYYY', 'BL1-AOI-01-Real-tact-time',
                 'BL1-AOI-01-Ref-Review-lamp-life-time', 'BL1-AOI-01-Trans-Review-lamp-life-time', 'BL1-AOI-01-lamp-life-time-Ref',
                 'BL1-AOI-01-lamp-life-time-Trans', 'BL1-AOI-01-Total-no-of-Ins-Sheet')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_cln_01':
        list1 = ('BL1-CLN-01-Glass-Loading-Time', 'BL1-CLN-01-Glass-Unloading-Time', 'BL1-CLN-01-Tact-Time',
                 'BL1-CLN-01-Process-Time', 'BL1-CLN-01-ConveyorSpeed', 'BL1-CLN-01-In-C/V-I/F-Speed',
                 'BL1-CLN-01-Out-C/V-I/F-Speed', 'BL1-CLN-01-R/B-1-1-Upper-Use-Unuse', 'BL1-CLN-01-R/B-1-1-Lower-Use-Unuse',
                 'BL1-CLN-01-R/B-1-2-Upper-Use-Unuse', 'BL1-CLN-01-R/B-1-2-Lower-Use-Unuse', 'BL1-CLN-01-R/B-1-1-Upper-Speed',
                 'BL1-CLN-01-R/B-1-1-Lower-Speed', 'BL1-CLN-01-R/B-1-2-Upper-Speed', 'BL1-CLN-01-R/B-1-2-Lower-Speed',
                 'BL1-CLN-01-Roll-Brush1-Gap', 'BL1-CLN-01-Roll-Brush3-Gap', 'BL1-CLN-01-Roll-Brush-Shower-Flow',
                 'BL1-CLN-01-Roll-Brush-Air-Curtain-CDA-Press', 'BL1-CLN-01-SBJ-Bath-SBJ-IN-Upper/Lower-Show',
                 'BL1-CLN-01-SBJ-Bath-SBJ-Out-Shower-Flow', 'BL1-CLN-01-SBJ-Bath-Shower-Flow', 'BL1-CLN-01-SBJ-Bath-SBJ-Upper-CDA-Pressure',
                 'BL1-CLN-01-SBJ-Bath-SBJ-Lower-CDA-Pressure', 'BL1-CLN-01-SBJ-Bath-Out-SBJ-CDA-Pressure', 'BL1-CLN-01-Fial-Rinse-Bath-Process-Shower-F',
                 'BL1-CLN-01-Fial-Rinse-&-Air-Knife-Bath-Proc', 'BL1-CLN-01-Air-Knife-Bath-Process-Upper-CDA', 'BL1-CLN-01-Air-Knife-Bath-Process-Lower-CDA',
                 'BL1-CLN-01-AIR-KNIFE-Bath-Process-Exhaust1', 'BL1-CLN-01-AIR-KNIFE-Bath-Process-Exhaust2', 'BL1-CLN-01-EQ-Driving-CDA-Pressure')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_coa_01':
        list1 = ('BL1-COA-01-Dispence_ACC1', 'BL1-COA-01-Dispence_time1', 'BL1-COA-01-Dispence_ACC2', 'BL1-COA-01-Dispence_time2',
                 'BL1-COA-01-Dispence_ACC3', 'BL1-COA-01-Dispence_time3', 'BL1-COA-01-Dispence_ACC4', 'BL1-COA-01-Dispence_time4',
                 'BL1-COA-01-Dispence_ACC5', 'BL1-COA-01-Dispence_time5', 'BL1-COA-01-SKBK_wait_time', 'BL1-COA-01-SKBK_ACC',
                 'BL1-COA-01-SKBK_time', 'BL1-COA-01-SKBK_DEC', 'BL1-COA-01-Dispense_Rate1', 'BL1-COA-01-Dispense_Rate2',
                 'BL1-COA-01-Dispense_Rate3', 'BL1-COA-01-Dispense_Rate4', 'BL1-COA-01-Dispense_Rate5', 'BL1-COA-01-SKBK_Rate',
                 'BL1-COA-01-Dispense_Vol', 'BL1-COA-01-Wait_time', 'BL1-COA-01-Gantry_ACC', 'BL1-COA-01-Gantry_DEC',
                 'BL1-COA-01-Gantry_Speed', 'BL1-COA-01-Wait_time_2nd', 'BL1-COA-01-Gantry_ACC_2nd', 'BL1-COA-01-Move_time_2nd',
                 'BL1-COA-01-Gantry_DEC_2nd', 'BL1-COA-01-Gantry_Speed_2nd', 'BL1-COA-01-Uncoat_area', 'BL1-COA-01-Bead_time',
                 'BL1-COA-01-Gap_ACC_1', 'BL1-COA-01-Gap_DEC', 'BL1-COA-01-Gap_Bead', 'BL1-COA-01-Gap_Coat',
                 'BL1-COA-01-Gap_End', 'BL1-COA-01-Rechrage_speed','BL1-COA-01-Rechrage_acc', 'BL1-COA-01-Gap_Before-Priming',
                 'BL1-COA-01-Dispence_time_Before-Priming', 'BL1-COA-01-VCD1_V1_Set_Vac', 'BL1-COA-01-VCD1_V2_Set_Vac',
                 'BL1-COA-01-VCD1_V3_Set_Vac', 'BL1-COA-01-VCD2_V1_Set_Vac', 'BL1-COA-01-VCD2_V2_Set_Vac',
                 'BL1-COA-01-VCD2_V3_Set_Vac', 'BL1-COA-01-VCD1_time', 'BL1-COA-01-VCD2_time')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_dev_01':
        list1 = ('BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-USE-TIME', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-NUMBER-OF-SUBSTRATE',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-USE-TIME', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-NUMBER-OF-SUBSTRATE',
                 'BL1-DEV-01-DEVELOPING(1)CHAMBER-SHOWER-FLOW', 'BL1-DEV-01-DEVELOPING(2)CHAMBER-SHOWER-FLOW',
                 'BL1-DEV-01-AIR-KNIFE-BLOW-UPPER-SIDE-DRY-AIR-FLOW', 'BL1-DEV-01-AIR-KNIFE-BLOW-LOWER-SIDE-DRY-AIR-FLOW',
                 'BL1-DEV-01-DEVELOPING(1)CHAMBER-SHOWER-PUMP-SHOWER-PRESSURE', 'BL1-DEV-01-DEVELOPING(2)CHAMBER-SHOWER-PUMP-SHOWER-PRESSURE',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-TEMPERATURE', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-CONDUCTIVITY',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-TEMPERATURE', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-CONDUCTIVITY')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_dhc_01':
        list1 = ('BL1-DHC-01-HP-START-TIME', 'BL1-DHC-01-HP-END-TIME', 'BL1-DHC-01-HP_ID', 'BL1-DHC-01-CP-START-TIME',
                'BL1-DHC-01-CP-END-TIME')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_duv_01':
        list1 = ('BL1-DUV-01-Conveyor-Speed', 'BL1-DUV-01-Lighting-Lamp', 'BL1-DUV-01-Lamp1-lifetime',
                 'BL1-DUV-01-Lamp2-lifetime', 'BL1-DUV-01-Lamp3-lifetime', 'BL1-DUV-01-Lamp4-lifetime')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_exp_01':
        list1 = ('BL1-EXP-01-Shot1-PA-Retry-Count', 'BL1-EXP-01-Shot1-Final-PA1', 'BL1-EXP-01-Shot1-Final-PA2',
                 'BL1-EXP-01-Shot1-Final-PA3', 'BL1-EXP-01-Shot1-Gap-Retry-Count', 'BL1-EXP-01-Shot1-Final-Gap-FR',
                 'BL1-EXP-01-Shot1-Final-Gap-FL', 'BL1-EXP-01-Shot1-Final-Gap-RL', 'BL1-EXP-01-Shot1-Final-Gap-RR',
                 'BL1-EXP-01-Shot1-Alignment-Retry-Count', 'BL1-EXP-01-Shot1-Final-Alignment-X', 'BL1-EXP-01-Shot1-Final-Alignment-Y',
                 'BL1-EXP-01-Shot1-Final-Alignment-T', 'BL1-EXP-01-Shot1-Final-Alignment-FRX', 'BL1-EXP-01-Shot1-Final-Alignment-FRY',
                 'BL1-EXP-01-Shot1-Final-Alignment-FLX', 'BL1-EXP-01-Shot1-Final-Alignment-FLY', 'BL1-EXP-01-Shot1-Final-Alignment-RLX',
                 'BL1-EXP-01-Shot1-Final-Alignment-RLY', 'BL1-EXP-01-Shot1-Final-Alignment-RRX', 'BL1-EXP-01-Shot1-Final-Alignment-RRY',
                 'BL1-EXP-01-Shot1-Expose-Accume', 'BL1-EXP-01-Shot2-PA-Retry-Count', 'BL1-EXP-01-Shot2-Final-PA1',
                 'BL1-EXP-01-Shot2-Final-PA2', 'BL1-EXP-01-Shot2-Final-PA3', 'BL1-EXP-01-Shot2-Gap-Retry-Count',
                 'BL1-EXP-01-Shot2-Final-Gap-FR', 'BL1-EXP-01-Shot2-Final-Gap-FL', 'BL1-EXP-01-Shot2-Final-Gap-RL',
                 'BL1-EXP-01-Shot2-Final-Gap-RR', 'BL1-EXP-01-Shot2-Alignment-Retry-Count', 'BL1-EXP-01-Shot2-Final-Alignment-X',
                 'BL1-EXP-01-Shot2-Final-Alignment-Y', 'BL1-EXP-01-Shot2-Final-Alignment-T', 'BL1-EXP-01-Shot2-Final-Alignment-FRX',
                 'BL1-EXP-01-Shot2-Final-Alignment-FRY', 'BL1-EXP-01-Shot2-Final-Alignment-FLX', 'BL1-EXP-01-Shot2-Final-Alignment-FLY',
                 'BL1-EXP-01-Shot2-Final-Alignment-RLX', 'BL1-EXP-01-Shot2-Final-Alignment-RLY', 'BL1-EXP-01-Shot2-Final-Alignment-RRX',
                 'BL1-EXP-01-Shot2-Final-Alignment-RRY', 'BL1-EXP-01-Shot2-Expose-Accume', 'BL1-EXP-01-Shot3-PA-Retry-Count',
                 'BL1-EXP-01-Shot3-Final-PA1', 'BL1-EXP-01-Shot3-Final-PA2', 'BL1-EXP-01-Shot3-Final-PA3',
                 'BL1-EXP-01-Shot3-Gap-Retry-Count', 'BL1-EXP-01-Shot3-Final-Gap-FR', 'BL1-EXP-01-Shot3-Final-Gap-FL',
                 'BL1-EXP-01-Shot3-Final-Gap-RL', 'BL1-EXP-01-Shot3-Final-Gap-RR', 'BL1-EXP-01-Shot3-Alignment-Retry-Count',
                 'BL1-EXP-01-Shot3-Final-Alignment-X', 'BL1-EXP-01-Shot3-Final-Alignment-Y', 'BL1-EXP-01-Shot3-Final-Alignment-T',
                 'BL1-EXP-01-Shot3-Final-Alignment-FRX', 'BL1-EXP-01-Shot3-Final-Alignment-FRY', 'BL1-EXP-01-Shot3-Final-Alignment-FLX',
                 'BL1-EXP-01-Shot3-Final-Alignment-FLY', 'BL1-EXP-01-Shot3-Final-Alignment-RLX', 'BL1-EXP-01-Shot3-Final-Alignment-RLY',
                 'BL1-EXP-01-Shot3-Final-Alignment-RRX', 'BL1-EXP-01-Shot3-Final-Alignment-RRY', 'BL1-EXP-01-Shot3-Expose-Accume',
                 'BL1-EXP-01-Shot4-PA-Retry-Count', 'BL1-EXP-01-Shot4-Final-PA1', 'BL1-EXP-01-Shot4-Final-PA2',
                 'BL1-EXP-01-Shot4-Final-PA3', 'BL1-EXP-01-Shot4-Gap-Retry-Count', 'BL1-EXP-01-Shot4-Final-Gap-FR',
                 'BL1-EXP-01-Shot4-Final-Gap-FL', 'BL1-EXP-01-Shot4-Final-Gap-RL', 'BL1-EXP-01-Shot4-Final-Gap-RR',
                 'BL1-EXP-01-Shot4-Alignment-Retry-Count', 'BL1-EXP-01-Shot4-Final-Alignment-X', 'BL1-EXP-01-Shot4-Final-Alignment-Y',
                 'BL1-EXP-01-Shot4-Final-Alignment-T', 'BL1-EXP-01-Shot4-Final-Alignment-FRX', 'BL1-EXP-01-Shot4-Final-Alignment-FRY',
                 'BL1-EXP-01-Shot4-Final-Alignment-FLX', 'BL1-EXP-01-Shot4-Final-Alignment-FLY', 'BL1-EXP-01-Shot4-Final-Alignment-RLX',
                 'BL1-EXP-01-Shot4-Final-Alignment-RLY', 'BL1-EXP-01-Shot4-Final-Alignment-RRX', 'BL1-EXP-01-Shot4-Final-Alignment-RRY',
                 'BL1-EXP-01-Shot4-Expose-Accume')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_ovn_01':
        list1 = ('BL1-OVN-01-OVEN-ID', 'BL1-OVN-01-SLOT-IN-OVEN', 'BL1-OVN-01-SLOT-IN-COOL', 'BL1-OVN-01-LD-IN-TIME-1',
                 'BL1-OVN-01-ULD-OUT-TIME-1', 'BL1-OVN-01-HEAT-TIME', 'BL1-OVN-01-COOL-TIME', 'BL1-OVN-01-TACT-TIME',
                 'BL1-OVN-01-PROCESS-OVEN-UP-TEMP', 'BL1-OVN-01-PROCESS-OVEN-LOW-TEMP', 'BL1-OVN-01-GLASS-MODE')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_phc_01':
        list1 = ('BL1-PHC-01-HP-START-TIME', 'BL1-PHC-01-HP-END-TIME', 'BL1-PHC-01-HP_ID',
                 'BL1-PHC-01-CP-START-TIME', 'BL1-PHC-01-CP-END-TIME')
        return list1
    elif table_name == 'h_spc_pdrd_bl1_sma_01':
        list1 = ('BL1-SMA-01-Total_Judge', 'BL1-SMA-01-Inspection-start-Time-1', 'BL1-SMA-01-Inspection-end-Time-1', 'BL1-SMA-01-Operator-ID-1')
        return list1
    else:
        return -1


def merge_data_fields(table_name):
    if table_name == 'bl1_aoi_01_merge':
        list1 = ('BL1-AOI-01-Real-tact-time', 'BL1-AOI-01-Ref-Review-lamp-life-time', 'BL1-AOI-01-Trans-Review-lamp-life-time',
                 'BL1-AOI-01-lamp-life-time-Ref', 'BL1-AOI-01-lamp-life-time-Trans', 'BL1-AOI-01-Total-no-of-Ins-Sheet',
                 'BL1-AOI-01-Carry-time-YYYY')
        return list1
    elif table_name == 'bl1_cln_01_merge':
        list1 = ('BL1-CLN-01-Tact-Time', 'BL1-CLN-01-Process-Time', 'BL1-CLN-01-ConveyorSpeed', 'BL1-CLN-01-In-C/V-I/F-Speed',
                 'BL1-CLN-01-Out-C/V-I/F-Speed', 'BL1-CLN-01-R/B-1-1-Upper-Use-Unuse', 'BL1-CLN-01-R/B-1-1-Lower-Use-Unuse',
                 'BL1-CLN-01-R/B-1-2-Upper-Use-Unuse', 'BL1-CLN-01-R/B-1-2-Lower-Use-Unuse', 'BL1-CLN-01-R/B-1-1-Upper-Speed',
                 'BL1-CLN-01-R/B-1-1-Lower-Speed', 'BL1-CLN-01-R/B-1-2-Upper-Speed', 'BL1-CLN-01-R/B-1-2-Lower-Speed',
                 'BL1-CLN-01-Roll-Brush1-Gap', 'BL1-CLN-01-Roll-Brush3-Gap', 'BL1-CLN-01-Roll-Brush-Shower-Flow',
                 'BL1-CLN-01-Roll-Brush-Air-Curtain-CDA-Press', 'BL1-CLN-01-SBJ-Bath-SBJ-IN-Upper/Lower-Show',
                 'BL1-CLN-01-SBJ-Bath-SBJ-Out-Shower-Flow', 'BL1-CLN-01-SBJ-Bath-Shower-Flow', 'BL1-CLN-01-SBJ-Bath-SBJ-Upper-CDA-Pressure',
                 'BL1-CLN-01-SBJ-Bath-SBJ-Lower-CDA-Pressure', 'BL1-CLN-01-SBJ-Bath-Out-SBJ-CDA-Pressure', 'BL1-CLN-01-Fial-Rinse-Bath-Process-Shower-F',
                 'BL1-CLN-01-Fial-Rinse-&-Air-Knife-Bath-Proc', 'BL1-CLN-01-Air-Knife-Bath-Process-Upper-CDA', 'BL1-CLN-01-Air-Knife-Bath-Process-Lower-CDA',
                 'BL1-CLN-01-AIR-KNIFE-Bath-Process-Exhaust1', 'BL1-CLN-01-AIR-KNIFE-Bath-Process-Exhaust2', 'BL1-CLN-01-EQ-Driving-CDA-Pressure',
                 'BL1-CLN-01-Glass-Time')
        return list1
    elif table_name == 'bl1_coa_01_merge':
        list1 = ('BL1-COA-01-Dispence_ACC1', 'BL1-COA-01-Dispence_time1', 'BL1-COA-01-Dispence_ACC2', 'BL1-COA-01-Dispence_time2',
                 'BL1-COA-01-Dispence_ACC3', 'BL1-COA-01-Dispence_time3', 'BL1-COA-01-Dispence_ACC4', 'BL1-COA-01-Dispence_time4',
                 'BL1-COA-01-Dispence_ACC5', 'BL1-COA-01-Dispence_time5', 'BL1-COA-01-SKBK_wait_time', 'BL1-COA-01-SKBK_ACC',
                 'BL1-COA-01-SKBK_time', 'BL1-COA-01-SKBK_DEC', 'BL1-COA-01-Dispense_Rate1', 'BL1-COA-01-Dispense_Rate2',
                 'BL1-COA-01-Dispense_Rate3', 'BL1-COA-01-Dispense_Rate4', 'BL1-COA-01-Dispense_Rate5', 'BL1-COA-01-SKBK_Rate',
                 'BL1-COA-01-Dispense_Vol', 'BL1-COA-01-Wait_time', 'BL1-COA-01-Gantry_ACC', 'BL1-COA-01-Gantry_DEC',
                 'BL1-COA-01-Gantry_Speed', 'BL1-COA-01-Wait_time_2nd', 'BL1-COA-01-Gantry_ACC_2nd', 'BL1-COA-01-Move_time_2nd',
                 'BL1-COA-01-Gantry_DEC_2nd', 'BL1-COA-01-Gantry_Speed_2nd', 'BL1-COA-01-Uncoat_area', 'BL1-COA-01-Bead_time',
                 'BL1-COA-01-Gap_ACC_1', 'BL1-COA-01-Gap_DEC', 'BL1-COA-01-Gap_Bead', 'BL1-COA-01-Gap_Coat',
                 'BL1-COA-01-Gap_End', 'BL1-COA-01-Rechrage_speed','BL1-COA-01-Rechrage_acc', 'BL1-COA-01-Gap_Before-Priming',
                 'BL1-COA-01-Dispence_time_Before-Priming', 'BL1-COA-01-VCD1_V1_Set_Vac', 'BL1-COA-01-VCD1_V2_Set_Vac',
                 'BL1-COA-01-VCD1_V3_Set_Vac', 'BL1-COA-01-VCD2_V1_Set_Vac', 'BL1-COA-01-VCD2_V2_Set_Vac',
                 'BL1-COA-01-VCD2_V3_Set_Vac', 'BL1-COA-01-VCD1_time', 'BL1-COA-01-VCD2_time')
        return list1
    elif table_name == 'bl1_dev_01_merge':
        list1 = ('BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-USE-TIME', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-NUMBER-OF-SUBSTRATE',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-USE-TIME', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-NUMBER-OF-SUBSTRATE',
                 'BL1-DEV-01-DEVELOPING(1)CHAMBER-SHOWER-FLOW', 'BL1-DEV-01-DEVELOPING(2)CHAMBER-SHOWER-FLOW',
                 'BL1-DEV-01-AIR-KNIFE-BLOW-UPPER-SIDE-DRY-AIR-FLOW', 'BL1-DEV-01-AIR-KNIFE-BLOW-LOWER-SIDE-DRY-AIR-FLOW',
                 'BL1-DEV-01-DEVELOPING(1)CHAMBER-SHOWER-PUMP-SHOWER-PRESSURE', 'BL1-DEV-01-DEVELOPING(2)CHAMBER-SHOWER-PUMP-SHOWER-PRESSURE',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-TEMPERATURE', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK1-CONDUCTIVITY',
                 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-TEMPERATURE', 'BL1-DEV-01-DEVELOPER-PROCESSING-TANK2-CONDUCTIVITY')
        return list1
    elif table_name == 'bl1_dhc_01_merge':
        list1 = ('BL1-DHC-01-HP_ID', 'BL1-DHC-01-HP-TIME', 'BL1-DHC-01-CP-TIME')
        return list1
    elif table_name == 'bl1_duv_01_merge':
        list1 = ('BL1-DUV-01-Conveyor-Speed', 'BL1-DUV-01-Lighting-Lamp', 'BL1-DUV-01-Lamp1-lifetime',
                 'BL1-DUV-01-Lamp2-lifetime', 'BL1-DUV-01-Lamp3-lifetime', 'BL1-DUV-01-Lamp4-lifetime')
        return list1
    elif table_name == 'bl1_exp_01_merge':
        list1 = ('BL1-EXP-01-Shot1-PA-Retry-Count', 'BL1-EXP-01-Shot1-Final-PA1', 'BL1-EXP-01-Shot1-Final-PA2',
                 'BL1-EXP-01-Shot1-Final-PA3', 'BL1-EXP-01-Shot1-Gap-Retry-Count', 'BL1-EXP-01-Shot1-Final-Gap-FR',
                 'BL1-EXP-01-Shot1-Final-Gap-FL', 'BL1-EXP-01-Shot1-Final-Gap-RL', 'BL1-EXP-01-Shot1-Final-Gap-RR',
                 'BL1-EXP-01-Shot1-Alignment-Retry-Count', 'BL1-EXP-01-Shot1-Final-Alignment-X', 'BL1-EXP-01-Shot1-Final-Alignment-Y',
                 'BL1-EXP-01-Shot1-Final-Alignment-T', 'BL1-EXP-01-Shot1-Final-Alignment-FRX', 'BL1-EXP-01-Shot1-Final-Alignment-FRY',
                 'BL1-EXP-01-Shot1-Final-Alignment-FLX', 'BL1-EXP-01-Shot1-Final-Alignment-FLY', 'BL1-EXP-01-Shot1-Final-Alignment-RLX',
                 'BL1-EXP-01-Shot1-Final-Alignment-RLY', 'BL1-EXP-01-Shot1-Final-Alignment-RRX', 'BL1-EXP-01-Shot1-Final-Alignment-RRY',
                 'BL1-EXP-01-Shot1-Expose-Accume', 'BL1-EXP-01-Shot2-PA-Retry-Count', 'BL1-EXP-01-Shot2-Final-PA1',
                 'BL1-EXP-01-Shot2-Final-PA2', 'BL1-EXP-01-Shot2-Final-PA3', 'BL1-EXP-01-Shot2-Gap-Retry-Count',
                 'BL1-EXP-01-Shot2-Final-Gap-FR', 'BL1-EXP-01-Shot2-Final-Gap-FL', 'BL1-EXP-01-Shot2-Final-Gap-RL',
                 'BL1-EXP-01-Shot2-Final-Gap-RR', 'BL1-EXP-01-Shot2-Alignment-Retry-Count', 'BL1-EXP-01-Shot2-Final-Alignment-X',
                 'BL1-EXP-01-Shot2-Final-Alignment-Y', 'BL1-EXP-01-Shot2-Final-Alignment-T', 'BL1-EXP-01-Shot2-Final-Alignment-FRX',
                 'BL1-EXP-01-Shot2-Final-Alignment-FRY', 'BL1-EXP-01-Shot2-Final-Alignment-FLX', 'BL1-EXP-01-Shot2-Final-Alignment-FLY',
                 'BL1-EXP-01-Shot2-Final-Alignment-RLX', 'BL1-EXP-01-Shot2-Final-Alignment-RLY', 'BL1-EXP-01-Shot2-Final-Alignment-RRX',
                 'BL1-EXP-01-Shot2-Final-Alignment-RRY', 'BL1-EXP-01-Shot2-Expose-Accume', 'BL1-EXP-01-Shot3-PA-Retry-Count',
                 'BL1-EXP-01-Shot3-Final-PA1', 'BL1-EXP-01-Shot3-Final-PA2', 'BL1-EXP-01-Shot3-Final-PA3',
                 'BL1-EXP-01-Shot3-Gap-Retry-Count', 'BL1-EXP-01-Shot3-Final-Gap-FR', 'BL1-EXP-01-Shot3-Final-Gap-FL',
                 'BL1-EXP-01-Shot3-Final-Gap-RL', 'BL1-EXP-01-Shot3-Final-Gap-RR', 'BL1-EXP-01-Shot3-Alignment-Retry-Count',
                 'BL1-EXP-01-Shot3-Final-Alignment-X', 'BL1-EXP-01-Shot3-Final-Alignment-Y', 'BL1-EXP-01-Shot3-Final-Alignment-T',
                 'BL1-EXP-01-Shot3-Final-Alignment-FRX', 'BL1-EXP-01-Shot3-Final-Alignment-FRY', 'BL1-EXP-01-Shot3-Final-Alignment-FLX',
                 'BL1-EXP-01-Shot3-Final-Alignment-FLY', 'BL1-EXP-01-Shot3-Final-Alignment-RLX', 'BL1-EXP-01-Shot3-Final-Alignment-RLY',
                 'BL1-EXP-01-Shot3-Final-Alignment-RRX', 'BL1-EXP-01-Shot3-Final-Alignment-RRY', 'BL1-EXP-01-Shot3-Expose-Accume',
                 'BL1-EXP-01-Shot4-PA-Retry-Count', 'BL1-EXP-01-Shot4-Final-PA1', 'BL1-EXP-01-Shot4-Final-PA2',
                 'BL1-EXP-01-Shot4-Final-PA3', 'BL1-EXP-01-Shot4-Gap-Retry-Count', 'BL1-EXP-01-Shot4-Final-Gap-FR',
                 'BL1-EXP-01-Shot4-Final-Gap-FL', 'BL1-EXP-01-Shot4-Final-Gap-RL', 'BL1-EXP-01-Shot4-Final-Gap-RR',
                 'BL1-EXP-01-Shot4-Alignment-Retry-Count', 'BL1-EXP-01-Shot4-Final-Alignment-X', 'BL1-EXP-01-Shot4-Final-Alignment-Y',
                 'BL1-EXP-01-Shot4-Final-Alignment-T', 'BL1-EXP-01-Shot4-Final-Alignment-FRX', 'BL1-EXP-01-Shot4-Final-Alignment-FRY',
                 'BL1-EXP-01-Shot4-Final-Alignment-FLX', 'BL1-EXP-01-Shot4-Final-Alignment-FLY', 'BL1-EXP-01-Shot4-Final-Alignment-RLX',
                 'BL1-EXP-01-Shot4-Final-Alignment-RLY', 'BL1-EXP-01-Shot4-Final-Alignment-RRX', 'BL1-EXP-01-Shot4-Final-Alignment-RRY',
                 'BL1-EXP-01-Shot4-Expose-Accume')
        return list1
    elif table_name == 'bl1_ovn_01_merge':
        list1 = ('BL1-OVN-01-OVEN-ID', 'BL1-OVN-01-SLOT-IN-OVEN', 'BL1-OVN-01-SLOT-IN-COOL',
                 'BL1-OVN-01-HEAT-TIME', 'BL1-OVN-01-COOL-TIME', 'BL1-OVN-01-TACT-TIME',
                 'BL1-OVN-01-PROCESS-OVEN-UP-TEMP', 'BL1-OVN-01-PROCESS-OVEN-LOW-TEMP',
                 'BL1-OVN-01-GLASS-MODE', 'BL1-OVN-01-LD-TIME-1')
        return list1
    elif table_name == 'bl1_phc_01_merge':
        list1 = ('BL1-PHC-01-HP_ID', 'BL1-PHC-01-HP-TIME',  'BL1-PHC-01-CP-TIME')
        return list1
    elif table_name == 'bl1_sma_01_merge':
        list1 = ('BL1-SMA-01-Total_Judge', 'BL1-SMA-01-Operator-ID-1',  'BL1-SMA-01-Inspection-Time-1')
        return list1
    else:
        return -1


def get_label():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    #取Processdata所有的Id, GLASS_ID
    cur.execute('select Id, GLASS_ID from bl1_01_overall_simple')
    glass_list = list(cur.fetchall())

    #取每一个glass_id对应的几个缺陷点，选择影响最大的
    cur_new = conn.cursor()
    cur_do = conn.cursor()
    score_standard = {'S': 1,
                      'M': 2,
                      'L': 10,
                      'O': 20}
    list_name = ['RB', 'RW', 'TB', 'TW', 'Other']
    list_score = ['S', 'M', 'L', 'O']
    for glass_id in glass_list:
        # 跳跃到目标点
        if glass_id[0] <= 4650:
            print(str(glass_id[0]) + ' jumped')
            continue

        strsql = 'select i.DefectNo, i.DefectAttrib from in_count_defect as i where i.MasterKey = ' \
                 'any(select in_count_m_oct_nov.MasterKey from in_count_m_oct_nov where in_count_m_oct_nov.custField9 = "'\
                 + glass_id[1] + '" and in_count_m_oct_nov.custField13 = "BL1")'
        cur_new.execute(strsql)

        #获取缺陷个数并判断是否为0
        defect_list = cur_new._rows
        defect_count = len(defect_list)
        if defect_count == 0:
            print(str(glass_id[0]) + ' jumped')
            continue

        #计分比较
        score = [0, 0, 0, 0, 0]
        name = ''
        atri = ''
        for i in range(defect_count):
            name = defect_list[i][0]
            atri = defect_list[i][1]
            if name in list_name:
                index = list_name.index(name)
                if atri in list_score:
                    score[index] = score[index] + score_standard[atri]
            elif len(name) > 0:
                score[4] = 100

        print('the ' + str(glass_id[0]) + 'th score list are ' + str(score))
        judge = list_name[score.index(max(score))]
        print('the judge is ' + judge)


        #上传记录
        strsql = 'update bl1_01_overall_simple set Label = "' + judge + '" where Id = ' + str(glass_id[0])
        cur_do.execute(strsql)
        conn.commit()
        print(str(glass_id[0]) + ' success')

    cur.close()
    cur_new.close()
    cur_do.close()
    conn.close()


def get_missing_defect():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    # 取Processdata所有的Id, GLASS_ID
    cur.execute('select Id, GLASS_ID from bl1_01_overall_simple')
    glass_list = list(cur.fetchall())

    cur_new = conn.cursor()
    list_masterkey = []
    count_masterkey = 0
    count_file_write = 0;
    for glass_id in glass_list:
        # 跳跃到目标点
        if glass_id[0] <= 502560:
            print(str(glass_id[0]) + ' jumped')
            continue

        strsql = 'select in_count_m_oct_nov.MasterKey from in_count_m_oct_nov where in_count_m_oct_nov.custField9 = "'\
                 + glass_id[1] + '" and in_count_m_oct_nov.custField13 = "BL1"'
        cur_new.execute(strsql)

        masterkey = cur_new._rows
        if len(masterkey) > 0:
            list_masterkey.append(masterkey[0][0])
            count_masterkey = count_masterkey + 1
            print(str(glass_id[0]) + ' success ' + str(count_masterkey) + ' already')
        if count_masterkey >= 1:
            dataframe = pd.DataFrame({'masterkey': list_masterkey})
            dataframe.to_csv('missing_defect.csv', index=False, sep=',')
            count_file_write = count_file_write + 1
            print('file write success ' + str(count_file_write) + ' times')
            count_masterkey = 0


def process_alert_info():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test', charset='utf8')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    #取Processdata所有的Id, GLASS_ID
    cur.execute('select Id, EQP_ID, ALERT_LEVEL, ALERT_CODE, ALERT_COMMENT, REPORT_TIME from h_eqp_alrt where REPORT_SOURCE = "BL1"')
    alert_list = list(cur.fetchall())

    total_list = []
    for alert in alert_list:
        #修改缺陷代码为10位
        list_piece = (list(alert))
        list_piece[3] = int(alert[3][0])*4096*4 + int(alert[3][-4:],16)
        #修改上传时间为数字
        string = list_piece[5]
        if string[-2:] == '上午':
            time_str = int(string[7:9])*365*24*3600 + int(string[3:5])*30*24*3600 + int(string[0:2])*24*3600 + int(string[10:12])*3600 + int(string[13:15])*60 + int(string[16:18])
            list_piece[5] = time_str
        elif string[-2:] == '下午':
            time_str = int(string[7:9])*365*24*3600 + int(string[3:5])*30*24*3600 + int(string[0:2])*24*3600 + (int(string[10:12]) + 12)*3600 + int(string[13:15])*60 + int(string[16:18])
            list_piece[5] = time_str
        #组合list
        total_list.append(list_piece)

    label = ['Id', 'EQP_ID', 'ALERT_LEVEL', 'ALERT_CODE', 'ALERT_COMMENT', 'REPORT_TIME']
    alert = pd.DataFrame(data=total_list, columns=label)
    alert.to_csv('E:/sharespace/DataProcess/raw_alert_info_new.csv', index=False)
    print('save success')


def get_time():
    try:
        conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='123456', db='test')
        cur = conn.cursor()
    except pymysql.Error:
        print("数据库连接异常")
        sys.exit(-1)

    # 取Processdata所有的Id, GLASS_ID
    cur.execute('select Id, GLASS_ID from bl1_01_overall_simple')
    glass_list = list(cur.fetchall())

    # 取每一个glass_id对应的几个缺陷点，选择影响最大的
    cur_new = conn.cursor()
    cur_do = conn.cursor()
    table_list = ['bl1_aoi_01_merge', 'bl1_cln_01_merge', 'bl1_coa_01_merge', 'bl1_dev_01_merge',
                  'bl1_dhc_01_merge', 'bl1_duv_01_merge', 'bl1_exp_01_merge', 'bl1_ovn_01_merge',
                  'bl1_phc_01_merge', 'bl1_sma_01_merge']
    field_list = ['AOI_Time', 'CLN_Time', 'COA_Time', 'DEV_Time', 'DHC_Time',
                  'DUV_Time', 'EXP_Time', 'OVN_Time', 'PHC_Time', 'SMA_Time']
    for i in range(10):
        # 跳跃到目标点
        if i <= 3:
            print(table_list[i] + ' jumped')
            continue

        count = 0
        for glass_id in glass_list:
            # 跳跃到目标点
            # if glass_id[0] <= 0:
            #     print(str(glass_id[0]) + ' jumped')
            #     continue

            current_table = table_list[i]
            current_field = field_list[i]
            strsql = 'select MEASURE_DATE from ' + current_table + ' where GLASS_ID = "' + glass_id[1] + '"'
            cur_new.execute(strsql)

            #获取缺陷个数并判断是否为0
            Date_time = cur_new._rows
            Date_time_count = len(Date_time)
            if Date_time_count == 0:
                print(str(glass_id[0]) + ' jumped')
                continue

            # 上传记录
            dt = Date_time[0][0].strftime("%Y-%m-%d %H:%M:%S")
            strsql_update = 'update bl1_01_overall_simple set ' + current_field + ' = "' + dt + '" where Id = ' + str(glass_id[0])
            cur_do.execute(strsql_update)
            count = count + 1
            if count >= 50:
                conn.commit()
                print(str(glass_id[0]) + ' success')
                count = 0

    cur.close()
    cur_new.close()
    cur_do.close()
    conn.close()



if __name__ == '__main__':
    print('begin:')
    # rows = data_preprocessing('h_spc_pdrd_bl1_phc_01', 'bl1_phc_01_merge')
    # print(str(rows) + ' records were processed')
    # time_processing()
    # process_data_merge()
    # process_data_simple_notnull()
    # process_data_scale_ajust('bl1_01_overall_simple', 20)           #大小调整，一般不使用，直接在数据集里面做
    # get_label()
    # get_missing_defect()
    # process_alert_info()
    get_time()
    pass
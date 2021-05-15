#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :Data_pre-processing.py
# @Time      :2021/5/11 4:02 下午
# @Author    :Kinddle

import pandas as pd
import numpy as np
from Database_init import *
import re
import logging
import os
import time


DIV_CONFIG = [(i, 0, 0) for i in range(25)]
# func_time = lambda st: [int(i) for i in re.split("[/ :]+", st)]

def func_time(st):
    return [int(i) for i in re.split("[/ :]+", st)]

def DiscreteByTime(raw, div_conf):
    length = len(div_conf[0])
    boxes = {i: [] for i in div_conf}
    time_value = lambda hour, minutes, second: hour * 3600 + minutes * 60 + second
    for i in raw.values:
        tmp_time = func_time(i[1])[-length:]
        for j in list(boxes.keys())[::-1]:
            if time_value(*j) < time_value(*tmp_time):
                boxes[j].append(i)
                break
    return boxes


def SubBoxes(data,time):
    tmp = data.iloc[0, :].copy()

    or_time = func_time(tmp["timestamp"])
    for ii in range(-1, -len(time) - 1, -1):
        or_time[ii] = time[ii]
    timestamp = "{}/{:02}/{:02} {:02}:{:02}:{:02}".format(*or_time)

    tmp["timestamp"] = timestamp

    try:
        tmp["value_raw"] = data["value_raw"].map(eval).mean()
    except Exception as Z:
        tmp["value_raw"] = "NaN"
        logger.debug(Z)
        logger.debug(tmp["value_raw"])

    try:
        tmp["value_hrf"] = data["value_hrf"].map(eval).mean()
    except Exception as Z:
        tmp["value_hrf"] = "NaN"
        logger.debug(Z)
        logger.debug(tmp["value_hrf"])

    return tmp


frame_name = pd.read_sql('SELECT name FROM sqlite_master WHERE "type"=="table"', engine)['name']
node_all = pd.read_sql('SELECT node_id, lat, lon FROM Tmp_node', engine)

if os.path.exists("pre_processing.log"):
    os.remove("pre_processing.log")

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
handler = logging.FileHandler("pre_processing.log")
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
console = logging.StreamHandler()
# console.setFormatter()
console.setLevel(logging.INFO)
logger.addHandler(handler)
logger.addHandler(console)

con = session.connection()
with con.begin():
    if np.sum(frame_name == "Discrete_Things"):
        logger.info(f'监测到{"Discrete_Things"}...')
        x = con.execute(f'Select count(*) from {"Discrete_Things"}')
        rownum = tuple(x)[0][0]
        logger.info(f'{"Discrete_Things"}的长度:{rownum}')
        # print(f'{"Discrete_Things"}的长度:{rownum}')
        if rownum != 0:
            con.execute(f'Delete from {"Discrete_Things"}')
            logger.info(f'检测到{"Discrete_Things"}已有数据,删除')
            # print(f'检测到{"Discrete_Things"}已有数据,删除')
    else:
        logger.info(f'未监测到{"Discrete_Things"}...')
session.commit()

DIV_BY_HOUR = True
if DIV_BY_HOUR == True:
    # 几十秒钟,仅按照小时划分
    t = time.time()
    dis_df = pd.read_sql("select timestamp,node_id,subsystem,sensor,parameter,"
                         "AVG(value_raw)as value_raw, AVG(value_hrf) as value_hrf, count(value_hrf) as count "
                         "from Things_locations_data "
                         "GROUP BY substring(timestamp,0,14),subsystem,sensor,parameter;", engine)
    dis_df.columns = list(dis_df.columns[:-2]) + ["value_raw", "value_hrf"]
    pd.io.sql.to_sql(frame=dis_df, name="Discrete_Things", con=engine, index=False, if_exists="append")
    logger.info("数据离散化完成~ 耗时{}s".format(time.time()-t))
else:
    # 效率稍低, 但是可以任意调整时间粒度 相同按小时产生24个分类需要大约100s
    t = time.time()
    all_date = pd.read_sql("select distinct substring(timestamp,0,11) from Things_locations_data;", engine)
    # all_node = pd.read_sql("select distinct 0,node_id,class from Tmp_node", engine)
    # all_para = pd.read_sql("select distinct 0,subsystem,sensor,parameter from Things_locations_data;", engine)
    # check_list = pd.merge(all_node, all_para, on="0").iloc[:, 1:]
    logger.info("查询准备完成")
    stmt = lambda condition: "select timestamp,node_id,subsystem,sensor,parameter," \
                             "AVG(value_raw) as value_raw,AVG(value_hrf) as value_hrf, count(value_hrf) as count  " \
                             "from Things_locations_data " \
                             "WHERE {} "\
                             "GROUP BY substring(timestamp,0,11),subsystem,sensor,parameter;".format(condition)
    The_div = ["{:02}:{:02}:{:02}".format(*i) for i in DIV_CONFIG]
    for up, down in zip(The_div[:-1], The_div[1:]):
        cond = [f"substring(timestamp,12)>'{up}'",
                f"substring(timestamp,12)<'{down}'"]
        tmp_list = pd.read_sql(stmt(" and ".join(cond)), engine)
        pd.io.sql.to_sql(frame=tmp_list, name="Discrete_Things", con=engine, index=False, if_exists="append")
        logger.info(f"finished  from {up} to {down}...")
    # for date, in all_date.values:
    #     logger.info(f"processing {date} ...")
    logger.info("数据离散化完成~ 耗时{}s".format(time.time() - t))


    #         # for check in check_list.values:
    #         #     cond = [f"timestamp>'{up}'",
    #         #             f"timestamp<'{down}'",
    #         #             f"node_id=='{check[0]}'",
    #         #             f"subsystem=='{check[2]}'",
    #         #             f"sensor=='{check[3]}'",
    #         #             f"parameter=='{check[4]}'",
    #         #             ]
    #         #     tmp_list = pd.read_sql(stmt(" and ".join(cond)), engine)
    #
    # # v1.0
    # for node in node_all["node_id"]:
    #     discrete_df = pd.DataFrame()
    #     raw_data = pd.read_sql(f'SELECT * FROM Things_locations_data WHERE node_id=="{node}" '
    #                            f'ORDER BY timestamp', engine)
    #
    #     # print(f"{node} loading")
    #     logger.info(f"{node} loading...")
    #     for subsystem in raw_data["subsystem"].unique():
    #         tmp_data_subsystem = raw_data[raw_data["subsystem"] == subsystem].copy()
    #         for sensor in tmp_data_subsystem["sensor"].unique():
    #             tmp_data_sensor = tmp_data_subsystem[raw_data["sensor"] == sensor].copy()
    #             for parameter in tmp_data_sensor["parameter"].unique():
    #                 tmp_data_parameter = tmp_data_sensor[raw_data["parameter"] == parameter].copy()
    #                 logger.debug(f"* node-> {node} subsystem->{subsystem} sensor->{sensor} parameter->{parameter}:\n"
    #                              f"----len:{len(tmp_data_parameter)}")
    #                 for date in tmp_data_parameter["timestamp"].map(lambda x: str(func_time(x)[:3])).unique():
    #                     tmp_data_time_date = \
    #                         tmp_data_parameter[tmp_data_parameter["timestamp"].
    #                                                map(lambda x: str(func_time(x)[:3])) == date].copy()
    #                     boxes = DiscreteByTime(tmp_data_parameter, DIV_CONFIG)
    #
    #                     tmp_df = pd.DataFrame(data={},
    #                                           columns=raw_data.columns)
    #                     for k in boxes:
    #                         if boxes[k]!=[]:
    #                             box_data = pd.DataFrame(boxes[k], columns=raw_data.columns)
    #
    #                             tmp_df = tmp_df.append(SubBoxes(box_data, k), ignore_index=True)
    #                     discrete_df = discrete_df.append(tmp_df, ignore_index=True)
    #     pd.io.sql.to_sql(frame=discrete_df, name="Discrete_Things", con=engine, index=False, if_exists="append")
    #     logger.info(f"{node} saved!")
    # logger.info("数据离散化完成~")

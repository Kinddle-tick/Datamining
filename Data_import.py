#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :Data_import.py
# @Time      :2021/5/9 7:06 下午
# @Author    :Kinddle

import pandas as pd
from Database_init import *

#  填写原始路径
Database_Things_Locations_Path = r"数据挖掘课程数据集/定位传感器大数据（Array of Things Locations）"
Database_Beach_Water_Quality_Path = r"数据挖掘课程数据集/天气自动传感器大数据（Beach Water Quality）挖掘"
#  生成or链接数据库
# engine = create_engine('sqlite:///Mydm.sqlite', echo=False)
# Session = sessionmaker(bind=engine)
# session=Session()

for The_csv, The_table in [(os.path.join(Database_Beach_Water_Quality_Path, "data.csv"), "Beach_water_quality_data"),
                        (os.path.join(Database_Things_Locations_Path, "nodes.csv"), 'Things_locations_node'),
                        (os.path.join(Database_Things_Locations_Path, "sensors.csv"), 'Things_locations_sensor'),
                        (os.path.join(Database_Things_Locations_Path, "data.csv"), 'Things_locations_data'),
                        ]:

    con = session.connection()
    with con.begin():
        x = con.execute(f"Select count(*) from {The_table}")
        rownum = tuple(x)[0][0]
        print(f"{The_table}的长度:{rownum}")
        if rownum != 0:
            con.execute(f"Delete from {The_table}")
            print(f"检测到{The_table}已有数据,删除")
    session.commit()

    print(f"输入<{The_csv}>的数据到<{The_table}>中...\n")
    csv_data = pd.read_csv(The_csv,encoding="UTF-8")
    pd.io.sql.to_sql(frame=csv_data,name=The_table,con=engine,index=False,if_exists="append")


print("数据库搭建完成")


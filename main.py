#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :main.py
# @Time      :2021/5/5 5:25 下午
# @Author    :Kinddle
from Database_init import *
from map_show_folium import show_map
import folium
import numpy as np
import pandas as pd


def pd_sql(sql):
    return pd.read_sql(sql, engine)


def sql_use(sql):
    con = session.connection()
    with con.begin():
        rtn = tuple(con.execute(sql))
    return rtn


# 用法1： 使用pd.read_sql执行语句和读取
frame_name = pd.read_sql('SELECT name FROM sqlite_master WHERE type=="table"', engine)['name']
print("table_name:")
print(frame_name)

# 用法2：不用pandas， 使用高效的'链接'思路完成
for name in frame_name:
    con = session.connection()
    with con.begin():
        x = con.execute(f"Select count(*) from {name}")
        rownum = tuple(x)
        print(f"{name}的长度:{rownum[0][0]}")
    session.commit()

# some codes
node_all = pd_sql("select * FROM Things_locations_node "
                  "WHERE node_id in (SELECT distinct node_id FROM Things_locations_data)"
                  "ORDER BY node_id")
DIV = [0, 1, 1, 8, 3, 3, 6, 7, 8, 4, 6, 2, 4, 6, 5, 4, 4, 2, 4, 2, 9, 4, 2, 6, 9, 2]

node_all["class"] = DIV
pd.io.sql.to_sql(frame=node_all,name="Tmp_node",con=engine,index=False,if_exists="replace")

lats = np.array([eval(i) for i in node_all.lat])
lons = np.array([eval(i) for i in node_all.lon])
lat, lon = 0.5*(max(lats)+min(lats)),0.5*(max(lons)+min(lons))

m = folium.Map(location=[lat, lon],
               zoom_start=11,
               # tiles='https://mt.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
               tiles='https://mt.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
               attr='default')
## 上面的可能要翻墙。。。

m.add_child(folium.LatLngPopup())

for node_id, lat, lon, address in node_all[["node_id","lat","lon","address"]].values:
    folium.Marker(
        location=[lat, lon],
        popup=f"{node_id}:\n{address}",
        icon=folium.Icon(icon='cloud')
    ).add_to(m)

show_map(m)


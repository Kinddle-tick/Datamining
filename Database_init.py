#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :Database_init.py
# @Time      :2021/5/8 8:44 下午
# @Author    :Kinddle

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, REAL, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

#  产生数据库基类
Base = declarative_base()
#  产生表单--Things_Locations:
class TL_data(Base):
    __tablename__ = 'Things_locations_data'
    id = Column(Integer, primary_key=True)
    timestamp = Column(String, unique=False)
    node_id = Column(String, ForeignKey('Things_locations_node.node_id'), unique=False)
    subsystem = Column(String, ForeignKey('Things_locations_sensor.subsystem'), unique=False)
    sensor = Column(String, ForeignKey('Things_locations_sensor.sensor'), unique=False)
    parameter = Column(String, ForeignKey('Things_locations_sensor.parameter'), unique=False)
    value_raw = Column(String)
    value_hrf = Column(String)

    The_node_info = relationship('TL_node', foreign_keys=[node_id], back_populates="The_data")
    # The_sensor_info = relationship('TL_sensor', foreign_keys=[subsystem, sensor, parameter], back_populates="The_data_2")
    def __repr__(self):
        return f"Table<{self.__tablename__}>" \
               f"\n| timestamp='{self.timestamp}'" \
               f"\n| node_id='{self.node_id}'," \
               f"\n| subsystem='{self.subsystem}'," \
               f"\n| sensor='{self.sensor}'" \
               f"\n| parameter='{self.parameter}'" \
               f"\n| value_raw='{self.value_raw}'" \
               f"\n| value_hrf='{self.value_hrf}'"

class TL_node(Base):
    __tablename__ = 'Things_locations_node'
    node_id = Column(String, primary_key=True)
    project_id = Column(String)
    vsn = Column(String)
    address = Column(String)
    lat = Column(String)
    lon = Column(String)
    description = Column(String)
    start_timestamp = Column(String)
    end_timestamp = Column(String)

    The_data = relationship("TL_data", back_populates="The_node_info")
    def __repr__(self):
        return f"Table<{self.__tablename__}>" \
               f"\n| node_id='{self.node_id}'" \
               f"\n| project_id='{self.project_id}'," \
               f"\n| vsn='{self.vsn}'," \
               f"\n| address='{self.address}'" \
               f"\n| lat='{self.lat}'" \
               f"\n| lon='{self.lon}'" \
               f"\n| description='{self.description}'" \
               f"\n| start_timestamp='{self.start_timestamp}'" \
               f"\n| end_timestamp='{self.end_timestamp}'"

class TL_sensor(Base):
    __tablename__ = 'Things_locations_sensor'
    ontology = Column(String)
    subsystem = Column(String, primary_key=True)
    sensor = Column(String, primary_key=True)
    parameter = Column(String, primary_key=True)
    hrf_unit = Column(String)
    hrf_minval = Column(String)
    hrf_maxval = Column(String)
    datasheet = Column(String)

    # The_data_2 = relationship("TL_data",foreign_keys=[subsystem, sensor, parameter], back_populates="The_sensor_info")
    def __repr__(self):
        return f"Table<{self.__tablename__}>" \
               f"\n| ontology='{self.ontology}'" \
               f"\n| subsystem='{self.subsystem}'," \
               f"\n| sensor='{self.sensor}'," \
               f"\n| parameter='{self.parameter}'" \
               f"\n| hrf_unit='{self.hrf_unit}'" \
               f"\n| hrf_minval='{self.hrf_minval}'" \
               f"\n| hrf_maxval='{self.hrf_maxval}'" \
               f"\n| datasheet='{self.datasheet}'"

#  产生表单--Beach_water_quality:
class BWQ_data(Base):
    __tablename__ = 'Beach_water_quality_data'
    Beach_Name = Column(String)
    Measurement_Date_And_Time = Column(String)
    Water_Temperature = Column(String)
    Turbidity = Column(String)
    Transducer_Depth = Column(String)
    Wave_Height = Column(String)
    Wave_Period = Column(String)
    Battery_Life = Column(String)
    Measurement_ID = Column(String, primary_key=True)
    def __repr__(self):
        return f"Table<{self.__tablename__}>" \
               f"\n| Beach_Name='{self.Beach_Name}'" \
               f"\n| Measurement_Date_And_Time='{self.Measurement_Date_And_Time}'," \
               f"\n| Water_Temperature='{self.Water_Temperature}'," \
               f"\n| Turbidity='{self.Turbidity}'" \
               f"\n| Transducer_Depth='{self.Transducer_Depth}'" \
               f"\n| Wave_Height='{self.Wave_Height}'" \
               f"\n| Wave_Period='{self.Wave_Period}'" \
               f"\n| Battery_Life='{self.Battery_Life}'" \
               f"\n| Measurement_ID='{self.Measurement_ID}'"


engine = create_engine('sqlite:///Mydm.sqlite')
Session = sessionmaker(bind=engine)
session = Session()

if __name__ == "__main__":
    #  若更改表结构， 需要删除重建
    engine = create_engine('sqlite:///Mydm.sqlite', echo=False)
    Base.metadata.drop_all(engine)
    os.remove("Mydm.sqlite")  # 物理删除
    #  生成or链接数据库
    engine = create_engine('sqlite:///Mydm.sqlite', echo=False)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    TL_data_in = TL_data(timestamp = "2020/02/10 00:00:01",node_id = "001e06112e77",subsystem="lightsense",
                         sensor="apds_9006_020",parameter="intensity",value_raw="65535",value_hrf="5267.409")
    TL_node_in = TL_node(node_id = "001e06112e77")

import os, sys
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy import create_engine
import csv
import sqlite3
engine = create_engine('sqlite:///database.db', echo=True)
conn = engine.connect()
print(engine.table_names())

meta = MetaData()

clean_measure= Table(
    'clean_measure',meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer)
    )


station = Table(
   'station', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('state', String),
)


meta.create_all(engine)










def create_connection(db_file):
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")

   finally:
       if conn:
           conn.close()

def convertor_to_dict(file_name):
    with open(os.path.join(sys.path[0], file_name), "r") as inp:
        reader = csv.reader(inp)
        a=0
        out=[]
        for row in reader:
            if a == 0:
                tag_list=row
            if a == 1:
                dict_={}
                i=0
                for tag in tag_list:
                    dict_.setdefault(tag, row[i])
                    i+=1
                out.append(dict_)
            a=1
        return out













ins=station.insert()
inst=clean_measure.insert()
conn.execute(ins,convertor_to_dict('clean_stations.csv'))
conn.execute(inst,convertor_to_dict('clean_measure.csv'))

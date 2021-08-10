from sqlalchemy import create_engine
import pymysql
import pandas as pdb
pymysql.install_as_MySQLdb()
import MySQLdb
import time
import pickle

engine = create_engine("mysql+mysqldb://root:[pw]@localhost:3306/mjkim", encoding='utf-8')
conn = engine.connect()
data_2019 = pickle.load(open("__X_2019_table.pickle","rb"))

len(data_2019) # 64305121

n=0
idx_start=n

while True :
    if idx_start==64305000:
        data=data_2019.iloc[idx_start:64305121]
        data.to_sql(name='data_2019', con=engine, if_exists='append', index=False)
        print('끝')
        break
    
    idx_end=idx_start+5000
    data=data_2019.iloc[idx_start:idx_end]
    data.to_sql(name='data_2019', con=engine, if_exists='append', index=False)
    time.sleep(0.001)
    idx_start=idx_end
    print(idx_end)
import pandas as pd
import numpy as np
from multiprocessing import Pool
import pickle
import tqdm
from time import time

import multiprocessing
multiprocessing.cpu_count()

# Y_Xs_count_dict=pickle.load(open("0707_sell_data_dict.pickle","rb"))
Y_Xs_type_count_dict_365567=pickle.load(open("aaa.pickle","rb"))
Xs_typ_table=pickle.load(open("aaa.pickle","rb"))

print(len(Y_Xs_count_dict))
print(len(Y_Xs_type_count_dict))

for i in Y_Xs_type_count_dict :
    try:
        pd.to_numeric(i)
    except:
        print(i)
        break

for i in Y_Xs_count_dict :
    print(i,Y_Xs_count_dict[i])
    break

for i in Y_Xs_type_count_dict :
    print(i,Y_Xs_type_count_dict[i])
    break

Xs_typ_table.head()

num_cores = 50

def df_making(dict_):
    df = pd.DataFrame(columns=list(Xs_typ_table['big_typ'].unique()))
    index = []
    for i in tqdm.tqdm(dict_) :
        data = Y_Xs_type_count_dict_365567[i]
        df = df.append(data,ignore_index=True)
        index.append(i)
    df.index=index
    return df

def split_dict(input_dict: dict, num_parts: int) -> list:
    list_len: int = len(input_dict)
    return [dict(list(input_dict.items())[i * list_len // num_parts:(i + 1) * list_len // num_parts])
        for i in range(num_parts)]

def parallelize(dic, func):
    dic_split = split_dict(dic, num_cores)
    pool = Pool(num_cores)
    result = pd.concat(pool.map(func, dic_split))
    pool.close()
    pool.join()
    return result

data=split_dict(Y_Xs_type_count_dict_365567,2)

data1=split_dict(data[0],2)
data2=split_dict(data[1],2)

data1_1=data1[0]
data1_2=data1[1]
data2_1=data2[0]
data2_2=data2[1]

start=time() # 353.9193482398987 # 8.846171140670776
result1=parallelize(data1_1,df_making)
print(time()-start)

start=time() #285.6164927482605 # 8.900442600250244
result2=parallelize(data1_2,df_making)
print(time()-start)

start=time() # 254.7865993976593 # 8.944982528686523
result3=parallelize(data2_1,df_making)
print(time()-start)

start=time() # 360.2212383747101 # 9.098328351974487
result4=parallelize(data2_2,df_making)
print(time()-start)

print(result1.shape)
print(result2.shape)
print(result3.shape)
print(result4.shape)

def df_1_making(df):
    df_1=pd.DataFrame(columns=df.columns)
    for i in tqdm.tqdm(range(len(df))):
        df_1=df_1.append(df.iloc[i]/df.iloc[i].sum())
    df_1.index=df.index
    return df_1

def parallelize(df, func):
    df_split=np.array_split(df, num_cores) 
    pool = Pool(num_cores)
    result = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return result

start=time() # 121.86266326904297 # 7.789303779602051
df_1_1=parallelize(result1,df_1_making)
time()-start

start=time() # 90.73159718513489 # 7.353627681732178
df_1_2=parallelize(result2,df_1_making)
time()-start

start=time() # 91.24309062957764 # 7.109282970428467
df_1_3=parallelize(result3,df_1_making)
time()-start

start=time() #87.9748957157135 # 8.001725196838379
df_1_4=parallelize(result4,df_1_making)
time()-start

df_1=pd.concat([df_1_1,df_1_2,df_1_3,df_1_4])

df_1.fillna(0,inplace=True)

df_1.to_csv('aaa.csv')

pd.read_csv('aaa.csv',index_col=0)

print(df_1.shape) # (3271555, 20)

df_1.reset_index(inplace=True) #pkl 일때는 drop=True를 빼준다 index가 저장되지 않기 때문이다.

pickle.dump(df_1, open("aaa.pickle","wb"))
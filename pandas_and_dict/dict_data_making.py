# 아마 세세분류까지는 의도에 따라 오차가 있을거 같으니까 # 한단계 윗단계로 적용하기

'''
모델링 적용 데이터 만들때 고려할 것
1. 건설사의 도급원가 케이스에서 nm_acctit contain 소모품, 교통비 => 고민하다가 일정한 양상 나타날것 같아서 포함하기로 판단
2. 기업은 거래건수 5개 이상으로 필터링
3. 맨 마지막에 gr_name1 unique()으로 항목 확인 
4. gr_name1 자본 일괄 삭제
'''

import os
import pandas as pd
from tqdm import tqdm

listdir=os.listdir('./매입_2019')
listdir.remove('회사')
listdir.sort()
listdir=listdir[1:]

data=pd.read_csv(os.path.join('./매입_2019/',listdir[0]))[['no_com','no_bisocial','ty_mth2','mn_bungae','gr_name1']]
data=data[data['gr_name1']!=('자본'  or '매출' or '기타')] # 매출원가, 도급, 분양, 제조만 남김

data['ty_mth2']=data['ty_mth2'].astype('int')

no_com_list=data['no_com'].unique()

mount={}
vat={}

'''
이 단계에서
dict={사업자주민번호:업종}만들기
deal[2]를 dict로 업종으로 환산하여 mount dict에 넣기
'''

for com in tqdm(no_com_list):
    if len(data[data['no_com']==com])>5:
        this_com=data[data['no_com']==com]
        
        for deal in this_com.values :
   
             # mount
            if deal[0] in mount :
                if deal[1] in mount[deal[0]]:
                    mount[deal[0]][deal[1]]+=deal[3]
                else:
                    mount[deal[0]][deal[1]]=deal[3]
            else:
                mount[deal[0]]={deal[1]:deal[3]}

#             # vat 건수 기준
#             if deal[0] in vat :
#                 if deal[2] in vat[deal[0]]:
#                     vat[deal[0]][deal[2]]+=1
#                 else:
#                     vat[deal[0]][deal[2]]=1
#             else:
#                 vat[deal[0]]={deal[2]:1}
                
             # vat 금액 기준

            if deal[0] in vat :
                if deal[2] in vat[deal[0]]:
                    vat[deal[0]][deal[2]]+=deal[3]
                else:
                    vat[deal[0]][deal[2]]=deal[3]
            else:
                vat[deal[0]]={deal[2]:deal[3]}
                



pd.options.display.float_format='{:.5f}'.format
df=pd.DataFrame(vat).T
df=df.fillna(0)

df_1=pd.DataFrame(columns=df.columns)
for idx, i in tqdm(zip(df.index,df.values)):
    df_1.loc[idx]=i/sum(i)


'''
51 과세 45% 중복없음(전부과세)
52 영세 0.5% 중복 두개씩 (영세한 자로써 부가가치세 면제)
53 면세 3% 중복 두개씩 (면세 상품으로써 부가가치세 면세)
54 불공 3% 
55 수입 0.1% 중복 두개인것도 있음 (면세 상품이 포함되어있어서인가..?)
56 금전 -
57 카과 41% 중복없음(전부과세) -> 과세로 편입 (51)
58 카면 1.4% 중복 두개씩 (면세 대상 재화를 카드를 구매하는 경우) -> 면세 편입 (53)
59 -
60 면건 0.02% 무증빙 면세 (한기업이 특이하게 쓰는듯함) -> 면세 편입 (53)
61 현과 2% 중복없음 (과세 재화를 현금으로 구매하는 경우) -> 과세로 편입 (51)
62 현면 0.2% 중복있음 (면세 재화를 현금으로 구매하는 경우) -> 면세 편입 (53)
63 복지 0.7% (한기업이 특이하게 쓰는듯함)
64 매세 -
==========================편입 이후
51 과세
52 영세 -> 영세의 경우 클러스터링에서 제외하는 방법도 있음
53 면세
55 수입
으로 축소해야함
'''
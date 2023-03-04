import datetime
import pandas as pd

if __name__ == '__main__':
    now = datetime.datetime.now()    
    collectedFilePath = "derived/ncav_{0}-{1:02d}-{2:02d}.tsv".format(now.year, now.month, now.day)

    collected = pd.read_csv(collectedFilePath, sep="\t")
    collected['code'] = collected['code'].map('{:06.0f}'.format)
    collected['유동자산'] = pd.to_numeric(collected['유동자산_2021/12'], errors='coerce')
    collected['부채'] = pd.to_numeric(collected['부채_2021/12'], errors='coerce')
    collected['시가총액(보통주,억원)'] = collected['시가총액(보통주,억원)'].str.replace(",", "")
    collected['시가총액(보통주,억원)'] = pd.to_numeric(collected['시가총액(보통주,억원)'], errors='coerce')
    collected['당기순이익'] = pd.to_numeric(collected['당기순이익_2022/09'], errors='coerce')
    collected = collected[~collected['유동자산'].isnull()]
    collected = collected[~collected['부채'].isnull()]

    collected['NCAV_R'] = (collected['유동자산'] - collected['부채']) / collected['시가총액(보통주,억원)']
    collected.sort_values(by = ['NCAV_R'], inplace=True, ascending=False)
    종목명최대길이  = collected['종목명'].str.len().max()
    collected['종목명'] = collected['종목명'].apply(lambda x : x.ljust(종목명최대길이))

    collected = collected[collected['NCAV_R'] > 0]
    collected = collected[collected['당기순이익'] > 0]

    

    print(collected[['code', 'NCAV_R', '종목명', '당기순이익', '시가총액(보통주,억원)', '유동자산', '부채', ]])

    output = collected[['code', 'NCAV_R', '종목명', '당기순이익', '시가총액(보통주,억원)', '유동자산', '부채', ]]

    output.to_csv('ncav_output_2023-03-04.tsv', sep="\t", index=False)



#     Unnamed: 0          0
# code               57
# 종목명                57
# 업종                 57
# PER                57
# PBR                57
# 시가총액(보통주,억원)       57
# 당기순이익_2022/09      67
# 유동자산_2021/12      119
# 부채_2021/12        119
# 유동자산_2022/03       67
# 부채_2022/03         67
# 유동자산_2022/06       67
# 부채_2022/06         67
# 유동자산_2022/09     2529
# 부채_2022/09       2529
# 당기순이익_2022/02    2572
# 당기순이익_2022/05    2572
# 당기순이익_2022/08    2572
# 당기순이익_2022/11    2572
# 유동자산_2022/02     2572
# 부채_2022/02       2572
# 유동자산_2022/05     2572
# 부채_2022/05       2572
# 유동자산_2022/08     2572
# 부채_2022/08       2572
# 당기순이익_2017/12    2580
# 당기순이익_2018/03    2580
# 당기순이익_2018/06    2580
# 당기순이익_2018/09    2580
# 유동자산_2017/12     2580
# 부채_2017/12       2580
# 유동자산_2018/03     2580
# 부채_2018/03       2580
# 유동자산_2018/06     2580
# 부채_2018/06       2580
# dtype: int64
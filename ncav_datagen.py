import datetime
import os

import pandas as pd

import kkmbaekkrx

# todo : https://medium.com/python-supply/map-reduce-and-multiprocessing-8d432343f3e7

now = datetime.datetime.now()    
collectedFilePath = "derived/ncav_{0}-{1:02d}-{2:02d}.tsv".format(now.year, now.month, now.day)

if not os.path.exists(collectedFilePath):
    krxStocks = kkmbaekkrx.getKrxStocks()
    # print(krxStocks)
    # print(kkmbaekkrx.parseFnguideFinance(kkmbaekkrx.getFnguideFinance('005930')))
    collected = pd.DataFrame()
    for _, stock_code in krxStocks.iterrows():
        try:
            snapshotHtml = kkmbaekkrx.getFnGuideSnapshot(stock_code.code)
            financeHtml = kkmbaekkrx.getFnguideFinance(stock_code.code)
            snapshot = kkmbaekkrx.parseFnguideSnapshot(snapshotHtml)
            finance = kkmbaekkrx.parseFnguideFinance(financeHtml)

            result = { **snapshot, **finance, 'code' : stock_code.code }
            collected = pd.concat([collected, pd.Series(result)])
            print(stock_code.code)
        except Exception as e:
            print(stock_code.code, "exception", e)

    print(collected)
    collected.to_csv(collectedFilePath, sep="\t")

collected = pd.read_csv(collectedFilePath, sep="\t")
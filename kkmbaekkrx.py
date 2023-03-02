import datetime
import logging
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame, ExcelWriter


def getKrxStocks():  
    code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]
    
    code_df.종목코드 = code_df.종목코드.map("{:06d}".format)
    code_df = code_df[['종목코드', '회사명', '업종', '주요제품']]
    code_df = code_df.rename(columns={'종목코드': 'code', '회사명':'name', '업종' : 'industry', '주요제품' : 'main_product'})

    return code_df

def getFnguideFinance(code):
    now = datetime.datetime.now()
    path = f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701"

    downloadedFileDirectory = 'derived/fnguide_finance_{0}-{1:02d}'.format(now.year, now.month)
    downloadedFilePath = '{0}/{1}.html'.format(downloadedFileDirectory, code)

    if not os.path.exists(downloadedFileDirectory):
        os.makedirs(downloadedFileDirectory)
    content = None
    if not os.path.exists(downloadedFilePath):
        response = requests.get(path)        
        with open(downloadedFilePath, "w", encoding="utf-8") as f:
            f.write(response.text)
        content = response.content  
    else:
        content = open(downloadedFilePath, "r", encoding = "utf-8").read()

    return content

def getFnGuideSnapshot(code):
    now = datetime.datetime.now()    
    path = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
    downloadedFileDirectory = 'derived/fnguide_snapshot_{0}-{1:02d}-{2:02d}'.format(now.year, now.month, now.day)
    downloadedFilePath = '{0}/{1}.html'.format(downloadedFileDirectory, code)

    if not os.path.exists(downloadedFileDirectory):
        os.makedirs(downloadedFileDirectory)
    content = None
    if not os.path.exists(downloadedFilePath):
        response = requests.get(path)        
        with open(downloadedFilePath, "w", encoding="utf-8") as f:
            f.write(response.text)
        content = response.content  
    else:
        content = open(downloadedFilePath, "r", encoding = "utf-8").read()

    return content

def parseFnguideFinance(content):
    logger = logging.getLogger("kkmbaekkrx")
    html = BeautifulSoup(content, 'html.parser')
    body = html.find('body')

    result = {}
    result['code'] = '??'
    result['종목명'] = html.select('#giName')[0].get_text()
    result['업종'] = html.select('#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span.stxt.stxt2')[0].get_text()
    result['PER'] = html.select('#corp_group2 > dl:nth-child(1) > dd')[0].get_text()
    result['PBR'] = html.select('#corp_group2 > dl:nth-child(4) > dd')[0].get_text()

    손익quarter헤더 = None
    당기순이익quarter = None
    if html.select_one('#divSonikQ') is not None:
        손익quarter헤더 = html.select_one('#divSonikQ').select_one('thead').select('th')
        for trs in html.select_one('#divSonikQ').select_one('tbody').select('tr'):
            rowName = trs.select_one('div')
            rowName = rowName.text if rowName else ''            
            rowName = rowName.replace(u"\xa0",u"")
            # print(rowName)
            if ("당기순이익" == rowName):
                당기순이익quarter = trs.select('th, td')


    for v in list(zip(손익quarter헤더,당기순이익quarter))[1:-1]:
        result["당기순이익_"+v[0].text] = v[1].text.replace(",", "")
        # print(v)

    대차quarter헤더 = None
    유동자산quarter = None
    부채quarter = None
    if html.select_one('#divDaechaQ') is not None:
        대차quarter헤더 = html.select_one('#divDaechaQ').select_one('thead').select('th')
        for trs in html.select_one('#divDaechaQ').select_one('tbody').select('tr'):
            rowName = trs.select_one('div')
            rowName = rowName.text if rowName else ''
            rowName = rowName.replace(u"\xa0",u"")            
            # print(rowName)
            if (rowName.startswith("유동자산")):
                유동자산quarter = trs.select('th, td')
            elif (rowName.startswith("부채")):
                부채quarter = trs.select('th, td')
    
    for v in list(zip(대차quarter헤더, 유동자산quarter, 부채quarter))[1:-1]:
        result["유동자산_"+v[0].text] = v[1].text.replace(",", "")
        result["부채_"+v[0].text] = v[2].text.replace(",", "")      
        # print(v)

    return result




def parseFnguideSnapshot(content):
    logger = logging.getLogger("kkmbaekkrx")
    html = BeautifulSoup(content, 'html.parser')
    body = html.find('body')

    result = {}
    result['code'] = '??'
    result['종목명'] = html.select('#giName')[0].get_text()
    result['업종'] = html.select('#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span.stxt.stxt2')[0].get_text()
    result['PER'] = html.select('#corp_group2 > dl:nth-child(1) > dd')[0].get_text()
    result['PBR'] = html.select('#corp_group2 > dl:nth-child(4) > dd')[0].get_text()    

    # marketcap
    for th in html.select('th'):
        anchor = th.select('a')
        if not anchor:
            continue
        if anchor[0].get_text() != "시가총액":
            continue
        span = th.select("span")
        if not span:
            continue
        if span[0].get_text() != "(보통주,억원)":
            continue

        for sibling in th.next_siblings:
            logger.info(sibling)
            if sibling.name == "td":
                result['시가총액(보통주,억원)'] = sibling.get_text()
                break
    # debt ratio
    # IFRS 연결/븐기
    if html.select_one('#highlight_D_Q') is not None:
        for th in html.select_one('#highlight_D_Q').select('th'):
            span = th.select('a')
            if span:
                one = 1
            else:
                continue
            if span[0].get_text() != '부채비율':
                continue           

            for sibling in th.next_siblings:
                logger.info(sibling)
                if sibling.name == 'td':
                    try:
                        result['부채비율'] = float(sibling.get_text())
                    except ValueError:
                        one = 1
                    
                    
    return result
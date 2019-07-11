# !/usr/bin/python
# coding:utf-8
import requests
from bs4 import BeautifulSoup as bs
import pandas
import pygsheets
from line_notify import lineNotifyMessage
import datetime
import schedule
import time

# Line notify
token = 'aXoGZw9UZCOaoux9n4NEzTyBhQdVPqLOI0UBBZTPJ5A' 

# config
area1 = ''
# area1 = 7
area2 = ''
minprice1 = ''
minprice2 = ''
# minprice2 = 20000000
hsimun = 'all'
# hsimun = '%BBO%A5_%A5%AB' #台北市




# computing time
starttime = datetime.datetime.now()
print(f'Start working...{starttime}')
# the first page 
pageNow = 1
url = f'http://aomp.judicial.gov.tw/abbs/wkw/WHD2A03.jsp?pageTotal=100&pageSize=15&rowStart=1&saletypeX=1&proptypeX=C52&courtX=TPD&order=odcrm&query_typeX=session&saleno=&hsimun={hsimun}&ctmd=all&sec=all&crmyy=&crmid=&crmno=&dpt=&saledate1=&saledate2=&minprice1={minprice1}&minprice2={minprice2}&sumprice1=&sumprice2=&area1={area1}&area2={area2}&registeno=&checkyn=all&emptyyn=all&order=odcrm&owner1=&landkd=&rrange=%A4%A3%A4%C0&comm_yn=&stopitem=&courtNoLimit=&pageNow={pageNow}'

try:
    res = requests.get(url)
except requests.exceptions.ConnectionError:
    print("Connection refused by the server..")
    print("Let me sleep for 5 seconds")
    print("ZZzzzz...")
    time.sleep(5)
    print("Was a nice sleep, now let me continue...")

soup = bs(res.text, 'html5lib')
tb = soup.select('table')[2]
df = pandas.read_html(tb.prettify(), header=0)[0]
df = df.drop(columns=['筆次'])
# print(df.head())

pageNow += 1

while True:
    # 2nd page, 3rd page, ...
    url = f'http://aomp.judicial.gov.tw/abbs/wkw/WHD2A03.jsp?pageTotal=100&pageSize=15&rowStart=1&saletypeX=1&proptypeX=C52&courtX=TPD&order=odcrm&query_typeX=session&saleno=&hsimun={hsimun}&ctmd=all&sec=all&crmyy=&crmid=&crmno=&dpt=&saledate1=&saledate2=&minprice1={minprice1}&minprice2={minprice2}&sumprice1=&sumprice2=&area1={area1}&area2={area2}&registeno=&checkyn=all&emptyyn=all&order=odcrm&owner1=&landkd=&rrange=%A4%A3%A4%C0&comm_yn=&stopitem=&courtNoLimit=&pageNow={pageNow}'
    try:
        res = requests.get(url)
    except requests.exceptions.ConnectionError:
        print("Connection refused by the server..")
        print("Let me sleep for 5 seconds")
        print("ZZzzzz...")
        time.sleep(5)
        print("Was a nice sleep, now let me continue...")
        continue
    soup = bs(res.text, 'html5lib')
    tb = soup.select('table')[2]
    df_newpage = pandas.read_html(tb.prettify(), header=0)[0]
    df_newpage = df_newpage.drop(columns=['筆次'])
    print('len(df_newpage):', len(df_newpage))
    # print('url:', url)
    
    if len(df_newpage) != 0:
        df = df.append(df_newpage, ignore_index=True)
        pageNow += 1
        print('len(df):', len(df))
    else:
        # print('len(df_newpage) = 0')
        break

gc = pygsheets.authorize()
sh = gc.open('HousePriceMonitor')

# wks = sh.add_worksheet("data")
wks = sh.worksheet_by_title("data")

# wks.set_dataframe(df,(1,1))
df_from_wks = wks.get_as_df()
# print('df_from_wks')
# print(df_from_wks.head())

new_data_count = 0

for index, row in df.iterrows():
    # access data using column names
    # print('iterrows:', index)
    # print('房屋地址/樓層面積:', row['房屋地址/樓層面積'])
    # print('拍賣日期  拍賣次數:', row['拍賣日期  拍賣次數'])
    # print('標 別:', row['標 別'])
    filter = (df_from_wks['房屋地址/樓層面積'] == row['房屋地址/樓層面積']) & (df_from_wks['拍賣日期  拍賣次數'] == row['拍賣日期  拍賣次數'])
    # print('iterrows:', index, row['字號  股別'])
    # filter = df_from_wks['字號  股別'] == row['字號  股別']

    df_dup = df_from_wks[filter]

    if len(df_dup) == 0:
        new_data_count += 1

        # row: from series to dataframe
        df_row = row.to_frame().T
        # insert logging time
        df_row.insert(0, '記錄時間', datetime.datetime.now())
        # Insert empty row
        wks.insert_rows(row=1, number=1)
        # new data append to Google sheet
        wks.set_dataframe(df_row, (1,1))
        print('New data is witten into Google sheet.')

        # trigger LINE notify
        message = f"\n法院名稱：{row['法院名稱']}\n字號  股別：\n{row['字號  股別']}\n拍賣日期次數：{row['拍賣日期  拍賣次數']}\n縣市：{row['縣市']}\n房屋地址/樓層面積：\n{row['房屋地址/樓層面積']}\n總拍賣底價(元)：{row['總拍賣底價(元)']}\n點交：{row['點交']}\n空屋：{row['空屋']}\n標 別：{row['標 別']}\n備 註：{row['備 註']}\n看圖：{row['看圖']}\n採通訊投標：{row['採通訊投標']}\n土地有無  遭受污染：{row['土地有無  遭受污染']}"
        lineNotifyMessage(token, message)

        # datetime
        endtime = datetime.datetime.now() 
        
    else:
        pass
        # print('Duplicated data!')
        # print('len:',len(df_dup))

print('new_data_count:', new_data_count)
# computing time
endtime = datetime.datetime.now() 

print(f'computing time: {(endtime - starttime).seconds} (sec)')


# def job():

# schedule.every().hour.at(':00').do(job)
# schedule.every().hour.at(':27').do(job)
# schedule.every().hour.do(job)

# schedule.every().day.at("09:00").do(job)
# schedule.every().day.at("12:00").do(job)
# schedule.every().day.at("15:00").do(job)
# schedule.every().day.at("18:00").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)
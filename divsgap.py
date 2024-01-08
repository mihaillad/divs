import os
import time
import numpy as np
import requests
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import glob
import csv
import pandas

tickers = []
stop_list = []
folder = "F:\\Python\\divs\\"


def get_page(url: int = None):

    r = requests.get(url)
    r.encoding = 'utf-8'
    return r.text


def save_to_file(text, fname):

    with open(fname,'w',encoding='utf-8') as file:
        file.write(text)


def save_to_csv(df, filename):
    with open(filename, 'w', newline='') as file: 
        writer = csv.writer(file)
        writer.writerows(df)       


def get_ticker_from_file(file):
    ticker = str(file)
    ticker = ticker.split("\\")[3]
    ticker = re.search(r'^\w+',ticker)[0]
    return ticker


def get_stop_list():
    file = "".join([folder, "stop_list.txt"])
    if len(glob.glob(file)) == 0:
        new_file = open(file,"w+")
        new_file.close

    stop_list = open(file,"r")
    stop_list = stop_list.read() 
    stop_list = stop_list.replace('\n', ' ').split()

    return stop_list 





def get_tickers():
    df = pandas.read_csv("".join([folder, "data.csv"]),delimiter=',')
    df = df.drop(["close_date","div_sum"],axis=1)
    df = df.groupby("ticker", as_index=False).sum()
    list = df["ticker"].tolist()

    print("Получили список тикеров")
    return list


def get_gap_data(days_after):

    today = datetime.today()

    df = pandas.read_csv("".join([folder, "data.csv"]),delimiter=',',parse_dates=['close_date'],dayfirst=True)
    df = df.drop(["div_sum"],axis=1)
    df = df.groupby(["ticker","close_date"], as_index=False).sum('')
    df["date_after_gap"] = df["close_date"]  + pandas.DateOffset(days=days_after)
    
    file = "".join([folder, "gap_data.csv"])
    if len(glob.glob(file)) == 0:
        gap_data = pandas.DataFrame({"ticker","close_date","close_price","date_after_gap","price_after_gap","change_price_after_gap"})
        gap_data.to_csv(folder+"gap_data.csv", index=False)

    gap_data = pandas.read_csv("".join([folder, "gap_data.csv"]),delimiter=',', parse_dates=['close_date','date_after_gap'],dayfirst=True)

    for index, row in df.iterrows():
        ticker = row["ticker"]

        if ticker in stop_list:     #Это для отброса не нужных бумаг
            continue
        
        if row["date_after_gap"] > today:     #Это для отброса будущих периодов
            continue
        
        data1 = (row["close_date"]).strftime('%Y-%m-%d')
        data2 = (row["date_after_gap"]).strftime('%Y-%m-%d')
        for gap_index,gap_row in gap_data.iterrows():
            if gap_row["ticker"] == ticker and gap_row["close_date"] == data1 :
                in_list = True
                date_after_gap = gap_row["date_after_gap"]
                price_after_gap = gap_row["price_after_gap"]
                change_price_after_gap = gap_row["change_price_after_gap"]
                break
            else:
                in_list = False

        if in_list == False:
            url = "".join(["https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/",ticker,".xml?from=",data1,"&till=",data2])
            text = get_page(url)
            soup = BeautifulSoup(text, 'lxml-xml')
            xml_rows = soup.find_all("row")
            if len(xml_rows)==0:
                print("".join(["Нет данных ", url])) 
                stop_list.append(ticker)
                save_to_file('\n'.join(stop_list),"".join([folder, "stop_list.txt"]))  
                continue

            close_date=row["close_date"].strftime('%Y-%m-%d')
            close_price = xml_rows[0].attrs["CLOSE"]
            if close_price=='':
                close_price = xml_rows[0].attrs["LEGALCLOSEPRICE"]

            if close_price=='':
                print("".join(["CLOSE=0 ", url]))   
                continue
            
            close_price = float(close_price)

            
            date_after_gap=row["date_after_gap"].strftime('%Y-%m-%d')
            price_after_gap = xml_rows[len(xml_rows)-1].attrs["CLOSE"]
            if price_after_gap=='':
                price_after_gap = xml_rows[len(xml_rows)-1].attrs["LEGALCLOSEPRICE"]

            if price_after_gap=='':
                print("".join(["CLOSE=0 ", url]))   
                continue
            
            price_after_gap = float(price_after_gap)
            if price_after_gap==0:
                continue
            
            change_price_after_gap = price_after_gap/close_price

            gap_data.loc[len(gap_data.index)] =[ticker, close_date, close_price, date_after_gap, price_after_gap, change_price_after_gap]
            gap_data.to_csv(folder+"gap_data.csv", index=False)
            time.sleep(2)



def get_current_data_from_file(tickers):

    current_data = []
    current_data.append(["ticker","price"])
    file = "".join([folder, "current_data.xml"]) 
    with open(file,encoding='utf-8') as f:
        price = 0
        response = f.read()
        soup = BeautifulSoup(response, 'lxml-xml')
        rows = soup.find_all("row")
        for ticker in tickers:

            for row in rows:
                SECID = row.attrs["SECID"]
                if SECID == ticker:
                    price = row.attrs["LCURRENTPRICE"]
                    current_data.append([ticker, price])

    save_to_csv(current_data, folder+"current_data.csv")
    print("Получили последние цены по списку тикеров")   

    return current_data


def prognoz():
    today = datetime.today()
    # one_year_later = today.replace(year = today.year-1)
    one_year_later = today-timedelta(days=330)
    df = pandas.read_csv("".join([folder, "data.csv"]),delimiter=',', parse_dates=['close_date'],dayfirst=True)

# Сгруппируем строки и суммируем дивы за одну дату, чтоб избавится от лишних строк
    df = df.groupby(["ticker","close_date"], as_index=False).sum('div_sum').sort_values(["ticker","close_date"], ascending=[True, False])

    df["next_close"] = df["close_date"]
    df["priority"] = 0
    df.loc[df["next_close"]>one_year_later, "priority"] = 1
    for i in range(0,3):
        df.loc[df["next_close"]<today, "next_close"] = df.loc[df["next_close"]<today, "next_close"] + pandas.DateOffset(years=1)

    df = df.sort_values(["ticker","priority","next_close"], ascending=[True,False,True])
    print("Экстраполируем прошлые даты закрытия реестра в будущее и отсортируем по убыванию")   


    # Оставим только первую строку тикера, остальные нам сейчас не нужны
    ticker = ""
    dropindex = []

    for index, row in df.iterrows():
        if row["ticker"] == ticker:
            dropindex.append(index)

        ticker = row["ticker"] 

    df = df.drop(index = dropindex)
    # df = df.drop("priority", axis=1)

    df.to_csv("".join([folder, "datanext.csv"]), index=False)
    print("Оставили только ближайшую дату закрытия реестра и сохранили в datanext.csv")   

    return df


def merge_divs_and_prices():
    #Не понял как конвертировать список в датафрейм с заголовками, поэтому сперва сохраню список в файл, а потом прочитаю в датфрейм
    dfprice = pandas.read_csv("".join([folder, "current_data.csv"]),delimiter=',')
    df = pandas.read_csv("".join([folder, "datanext.csv"]),delimiter=',', parse_dates=['close_date','next_close'],dayfirst=True)
    df = df.merge(dfprice, left_on="ticker", right_on="ticker")

    dfgap = pandas.read_csv("".join([folder, "gap_data.csv"]),delimiter=',')
    dfgap = dfgap.drop(["close_date","close_price","date_after_gap","price_after_gap"], axis=1)
    dfgap = dfgap.groupby(["ticker"], as_index=False).median("change_price_after_gap")
    df = df.merge(dfgap, left_on="ticker", right_on="ticker")



    today = datetime.today()
    df["days_left"] = (df["next_close"] - today)/np.timedelta64(1,"D") #Осталось дней
    df["CY"] = (df["div_sum"] / df["price"]*100*df["change_price_after_gap"]) #Текущая доходность с учетом дивидендного гэпа
    df["AY"] = (df["CY"] / (df["days_left"]+30) * 365) #Годовая доходность
    df = df.sort_values(["priority","AY","CY"], ascending=[False,False,False])

    df["ratio"] = float(0) #Рекомендуемая доля
    all_ratio = 100 #Доля див.акций от общего портфеля
    limit_ratio = 10 #Максимальная доля одного эмитента
    step = 0.5 #Шаг уменьшения доли
    for index, row in df.iterrows():
        # row["ratio"] = limit_ratio*all_ratio/100
        df.at[index,"ratio"] = limit_ratio*all_ratio/100
        
        limit_ratio = max(limit_ratio-step,0)

        # summa = df["ratio"].sum()
        # if summa >= all_ratio:
        #     break

    
    df["link1"] = "https://yandex.ru/search/?text="
    df["link2"] = "+дивиденды&lr=50"
    df["link"] = df[["link1","ticker","link2"]].apply(''.join, axis=1)
    df = df.drop(["link1","link2","change_price_after_gap","days_left","priority"], axis=1)


    df.to_csv(folder+"ratio.csv", index=False)
    print("Объединили прогнозные дивы и текущие цены")

    return df



only_ticker = "" #RTKM MTSS DSKY LNZL POSI
stop_list = get_stop_list()


# Подготовить таблицу цен в дату закрытия и через несколько дней
days_after = 30
get_price_data(days_after)
# current_data = get_price_data_from_file(tickers)

# # Объединим таблицы текущих цен и дивидендов
df = merge_divs_and_prices()











      







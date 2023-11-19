import os
import numpy as np
import requests
import re
from datetime import datetime
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


def get_list():
    headers = {
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 YaBrowser/23.7.5.717 Yowser/2.5 Safari/537.36"
    }

    base_url = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai/_/"

    response = requests.get(base_url, headers=headers).text

    soup = BeautifulSoup(response, 'lxml')

    link = soup.find(
    "div", id="widget-ac0a9ff2-4f9f-8017-d837-5683009655d0").find_all("a", class_="link")

    list_link = []
    for i in link:
        link_href = i.get("href").replace("..", "")
        ticks = i.find_all("span")
        for ticker in ticks:
            if ticker == None:
                continue

            ticker = ticker.text.replace("(", "")
            if ticker == "":
                continue

            ticker = ticker.replace(")", "").upper()
            if ticker.isalpha() == False:
                continue
       
            # print(f"https://закрытияреестров.рф{link_href}")
            # Добавляем полученные ссылки в список, который в последующем можно обойти и спарсить каждую страницу
            list_link.append([ticker,f"https://закрытияреестров.рф{link_href}"])

    print("Получили список ссылок list_link")
    return list_link


def get_divinfo_to_files(list_link):
    cnt = 0
    for i in list_link:
        ticker = i[0]
        if only_ticker != "" and only_ticker != ticker:     #Это для теста одного тикера
            continue
    
        if ticker in stop_list:     #Это для отброса не нужных бумаг
            continue
        
        file = "".join([folder, ticker, ".html"])
        if os.path.isfile(file):
            mtime = datetime.fromtimestamp(os.path.getmtime(file))
            a = datetime.now() - mtime
            if a.days < 5:
                continue

        text = get_page(i[1])
        save_to_file(text, file)
        cnt +=1

    print(" ".join(["Сохранили страницы по ссылкам в файлы:",str(cnt)]))


def get_divlist_from_files():
    files = glob.glob("".join([folder, "*.html"]))
    data = []
    data.append(["ticker","close_date","div_sum"])
    for file in files:
         with open(file,encoding='utf-8') as f:
             get_table_from_file(file, data)

    save_to_csv(data, folder+"data.csv")
    print("Получили данные из файлов в data.csv")


def get_table(link, data): #Не используется
#Получить на выходе таблицу с колонками Тикер, сумма, дата 
    base_url = "https://xn--80aeiahhn9aobclif2kuc.xn--p1ai/AFLT/"
    response = requests.get(base_url, headers=headers).text

    soup = BeautifulSoup(response, 'lxml')

    ticker = link.split("/")[3]
    div_sum = 0
    table = soup.find("table")
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    for row in rows:
         cols = row.find_all("td")
         cols = [ele.text.strip() for ele in cols]
         close_date = cols[0]
         close_date = re.search(r'[0-9]{2}.[0-9]{2}.[0-9]{4}',close_date)
         if close_date != None:
            close_date = close_date.group(0)
            close_date = datetime.strptime(close_date, "%d.%m.%Y")
            div_sum = cols[1]
            div_sum = re.search(r'[\d, ]+',div_sum)
            div_sum = float(div_sum.group(0).replace(" ","").replace(",","."))

         else:
            continue
        

         data.append([ticker, close_date, div_sum])


def get_table_from_file(file, data):
#Получить на выходе таблицу с колонками Тикер, сумма, дата 
    with open(file,encoding='utf-8') as f:
        response = f.read()

    soup = BeautifulSoup(response, 'lxml')

    ticker = get_ticker_from_file(file)
    if only_ticker != "" and only_ticker != ticker:     #Это для теста одного тикера
        return
    
    if ticker in stop_list:     #Это для отброса не нужных бумаг
        return
   
    div_sum = 0
    table = soup.find("table")
    table_body = table.find("tbody")
    rows = table_body.find_all("tr")
    for row in rows:
         cols = row.find_all("td")
         cols = [ele.text.strip() for ele in cols]
         close_date = re.search(r'[0-9]{2}.[0-9]{2}.[0-9]{4}',cols[0])
         if close_date == None: #Проверка на правильность написания даты
            close_date = re.search(r'[0-9]{1}.[0-9]{2}.[0-9]{4}',cols[0])
             
         if close_date != None:
            close_date = close_date.group(0)

            a = datetime.now() - datetime.strptime(close_date, "%d.%m.%Y")
            if a.days > 1000:
                continue

            else:

                div_sum = cols[1]
                div_sum = re.search(r'[\d, ]+',div_sum)
                div_sum = div_sum.group(0) 
                if div_sum != ' ':
                    div_sum = float(div_sum.replace(" ","").replace(",","."))

                else:
                    div_sum  = 0

                # Проверим префы
                if len(cols) == 3:
                    div_sumP = cols[2]
                    div_sumP = re.search(r'[\d, ]+',div_sumP)
                    div_sumP = div_sumP.group(0) 
                    if div_sumP != ' ':
                        div_sumP = float(div_sumP.replace(" ","").replace(",","."))
                        tickerP = "".join([ticker,"P"])
                        data.append([tickerP, close_date, div_sumP])


         else:
            continue
        

         data.append([ticker, close_date, div_sum])


def get_tickers():
    df = pandas.read_csv("".join([folder, "data.csv"]),delimiter=',')
    df = df.drop(["close_date","div_sum"],axis=1)
    df = df.groupby("ticker", as_index=False).sum()
    list = df["ticker"].tolist()

    print("Получили список тикеров")
    return list


def get_current_data():
    url = "http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.xml?iss.meta=off&iss.only=marketdata"
    text = get_page(url)
    save_to_file(text, "".join([folder, "current_data.xml"])) 
    print("Получили текущие данные с биржи")   


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
    one_year_later = today.replace(year = today.year-1)
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


    today = datetime.today()
    df["days_left"] = (df["next_close"] - today)/np.timedelta64(1,"D") #Осталось дней
    df["CY"] = (df["div_sum"] / df["price"]*100) #Текущая доходность
    df["AY"] = (df["CY"] / df["days_left"] * 365) #Годовая доходность
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
    df = df.drop(["link1","link2"], axis=1)


    df.to_csv(folder+"ratio.csv", index=False)
    print("Объединили прогнозные дивы и текущие цены")

    return df


only_ticker = "" #RTKM MTSS DSKY LNZL POSI
stop_list = get_stop_list()

# Получить список диивидендных акций
list_link = get_list() 

# Получить информацию о дивидендах по списку акций и сохранить каждый в отдельный файл
get_divinfo_to_files(list_link)

# Собрать информацию с файлов в список data. Сохранить data  в csv
get_divlist_from_files()

# Получить список тикеров
tickers = get_tickers()

# Спрогнозируем прошедшие даты закрытия в будущее и оставим только предстоящие события
df = prognoz()

# Подготовить таблицу текущих цен
get_current_data()
current_data = get_current_data_from_file(tickers)

# Объединим таблицы текущих цен и дивидендов
df = merge_divs_and_prices()

# Отправим данные в телегу











      







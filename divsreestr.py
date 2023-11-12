import requests
import re
from datetime import datetime
from bs4 import BeautifulSoup
import glob
import csv
import pandas

tickers = []
folder = "F:\\Python\\divs\\"


def get_page(url: int = None):

    r = requests.get(url)
    r.encoding = 'utf-8'
    return r.text


def save_to_file(text, fname):

    with open(fname,'w',encoding='utf-8') as file:
        file.write(text)


def get_ticker_from_file(file):
    ticker = str(file)
    ticker = ticker.split("\\")[3]
    ticker = re.search(r'^\w+',ticker)[0]
    return ticker


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
    # print(f"https://закрытияреестров.рф{link_href}")
    # Добавляем полученные ссылки в список, который в последующем можно обойти и спарсить каждую страницу
        list_link.append(f"https://закрытияреестров.рф{link_href}")

    return list_link


def get_table(link, data):
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

         else:
            continue
        

         data.append([ticker, close_date, div_sum])


def get_tickers():
    df = pandas.read_csv('F:\\Python\\divs\\data.csv',delimiter=',')
    df = df.drop(["close_date","div_sum"],axis=1)
    df = df.groupby("ticker", as_index=False).sum()
    list = df["ticker"].tolist()

    return list


def save_to_csv(df, filename):
    with open(filename, 'w', newline='') as file: 
        writer = csv.writer(file)
        writer.writerows(df)       


def get_current_data(ticker):
    url = "http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.xml?iss.meta=off&iss.only=marketdata"
    text = get_page(url)
    save_to_file(text, "".join([folder, "current_data.xml"]))    


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

    return current_data


def prognoz():
    today = datetime.today()
    one_year_later = today.replace(year = today.year-1)
    df = pandas.read_csv('F:\\Python\\divs\\data.csv',delimiter=',', parse_dates=['close_date'],dayfirst=True)

# Сгруппируем строки и суммируем дивы за одну дату, чтоб избавится от лишних строк
    df = df.groupby(["ticker","close_date"], as_index=False).sum('div_sum').sort_values(["ticker","close_date"], ascending=[True, False])

    df["next_close"] = df["close_date"]
    df["priority"] = 0
    df.loc[df["next_close"]>one_year_later, "priority"] = 1
    for i in range(0,3):
        df.loc[df["next_close"]<today, "next_close"] = df.loc[df["next_close"]<today, "next_close"] + pandas.DateOffset(years=1)

    df = df.sort_values(["ticker","priority","next_close"], ascending=[True,False,True])

    # Оставим только первую строку тикера, остальные нам сейчас не нужны
    ticker = ""
    dropindex = []

    # print(df1)
    for index, row in df.iterrows():
        if row["ticker"] == ticker:
            dropindex.append(index)

        ticker = row["ticker"] 

    df = df.drop(index = dropindex)
    print(dropindex)
    df.to_csv(folder+"datanext.csv")

    return df

# Получить список диивидендных акций
# list_link = get_list() 

# Получить информацию о дивидендах по списку акций и сохранить каждый в отдельный файл
# for i in list_link:
#     text = get_page(i)
#     ticker = i.split("/")[3]
#     save_to_file(text, "".join([folder, ticker, ".html"]))

# Собрать информацию с файлов в список data. Сохранить data  в csv
# files = glob.glob("".join([folder, "*.html"]))
# data = []
# data.append(["ticker","close_date","div_sum"])
# for file in files:
#      with open(file,encoding='utf-8') as f:
#          get_table_from_file(file, data)

# save_to_csv(data, folder+"data.csv")

# Получить список тикеров
# tickers = get_tickers()

# Подготовить таблицу текущих цен
# current_data = get_current_data_from_file(tickers)

# Спрогнозируем прошедшие даты закрытия в будущее и оставим только предстоящие события
df = prognoz()








      







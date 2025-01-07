import requests
import csv
from bs4 import BeautifulSoup
import json
import pandas as pd

tickers = []
stop_list = []
folder = "F:\\Python\\divs\\"


def save_to_file(text, fname):

    with open(fname,'w',encoding='utf-8') as file:
        file.write(text)

        
def save_to_csv(df, filename):
    with open(filename, 'w', newline='') as file: 
        writer = csv.writer(file)
        writer.writerows(df)  



def get_page_from_bcs():

    url = "https://api.bcs.ru/divcalendar/v1/partner/dividends?order=0&sorting=2&actual=1&year=2025&limit=50"
    headers = {
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "User-Agent":
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 YaBrowser/23.7.5.717 Yowser/2.5 Safari/537.36"
    }

    text = requests.get(url, headers=headers).text
    save_to_file(text, "".join([folder, "BCSkalendar.txt"]))




def merge_df():
    with open("".join([folder, "BCSkalendar.txt"]),encoding='utf-8') as f:
        data = json.load(f)

        dfnew = pd.DataFrame(data["data"], columns=['secureCode','closingDate','dividendValue'])
        dfnew.rename(columns={'secureCode':'ticker','closingDate':'close_date','dividendValue':'div_sum'}, inplace=True)
        dfnew['close_date'] = pd.to_datetime(dfnew['close_date']).dt.normalize() 

        df = pd.read_csv("".join([folder, "data.csv"]),delimiter=',',parse_dates=['close_date'],dayfirst=False)
        df = pd.concat([df,dfnew],axis=0, join="outer", sort=True).drop_duplicates()
        df = df.groupby(["ticker","close_date","div_sum"], as_index=False).sum().sort_values(["ticker","close_date"], ascending=[True, False])
        df.to_csv(folder+"data.csv", index=False)



get_page_from_bcs() 
merge_df()
import requests
import json
import os
import time
from pathlib import Path
import pandas as pd


def get_file(url, file_name):
    need_download = False
    max_age_seconds = 3600

    if not os.path.exists(file_name):
        print(f"Файл {file_name} не найден.")
        need_download = True
    else:
        # Получаем время последнего изменения файла
        file_mtime = os.path.getmtime(file_name)
        current_time = time.time()
        age_seconds = current_time - file_mtime

        if age_seconds > max_age_seconds:
            print(f"Файл {file_name} устарел (возраст: {age_seconds:.0f} сек., лимит: {max_age_seconds} сек.).")
            need_download = True
        else:
            print(f"Файл {file_name} актуален (возраст: {age_seconds:.0f} сек.).")

    if need_download:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            # Явно указываем кодировку UTF-8 для корректной записи кириллицы
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    else:
        print("Загрузка не требуется.")


bonds_yield_min = 21
bonds_yield_max = 60
bonds_price_min = 60
bonds_price_max = 96
bonds_duration_min = 6
bonds_duration_max = 120
risk_free_rate = 15


# Директория для сохранения файлов
output_dir = r"./data"
os.makedirs(output_dir, exist_ok=True)

# Получим текущие цены ОБЛИГАЦИЙ с Мосбиржи
boards = ["TQIR", "TQCB", "TQOD", "TQOB"]
base_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{boardgroup}/securities.json"
params = {
    "iss.meta": "off",
    "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,ACCRUEDINT,LATNAME",
    "marketdata.columns": "SECID,BOARDID,YIELD,DURATION,LCURRENTPRICE,LAST",
}

for board in boards:
    url = base_url.format(boardgroup=board)
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(output_dir, f"prices_{board}.json")
        # Явно указываем кодировку UTF-8 для корректной записи кириллицы
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Цены по площадке {board} успешно сохранены в {file_path}")
    else:
        print(
            f"Ошибка при запросе цен для площадки {board}: код ответа {response.status_code}"
        )


# Получим текущие цены АКЦИЙ с Мосбиржи
boards = ["TQTF", "TQBR"]
base_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/{boardgroup}/securities.json"
params = {
    "iss.meta": "off",
    "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,ACCRUEDINT,LATNAME",
    "marketdata.columns": "SECID,BOARDID,YIELD,DURATION,LCURRENTPRICE,LAST",
}

for board in boards:
    url = base_url.format(boardgroup=board)
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(output_dir, f"prices_{board}.json")
        # Явно указываем кодировку UTF-8 для корректной записи кириллицы
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Цены по площадке {board} успешно сохранены в {file_path}")
    else:
        print(
            f"Ошибка при запросе цен для площадки {board}: код ответа {response.status_code}"
        )


base_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boardgroups/{boardgroup}/securities.json"
params = {
    "iss.meta": "off",
    "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,MATDATE,ACCRUEDINT,LATNAME",
    "marketdata.columns": "SECID,YIELD,DURATION",
}

# Список интересующих площадок
boards = [58, 105, 245]  # 207,


for board in boards:
    url = base_url.format(boardgroup=board)
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        file_path = os.path.join(output_dir, f"{board}.json")
        # Явно указываем кодировку UTF-8 для корректной записи кириллицы
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Данные по площадке {board} успешно сохранены в {file_path}")
    else:
        print(
            f"Ошибка при запросе данных для площадки {board}: код ответа {response.status_code}"
        )


# Создаем датафрейм

# Списки для хранения DataFrame
securities_dfs = []
marketdata_dfs = []

for board in boards:
    file_path = Path(os.path.join(output_dir, f"{board}.json"))
    if not file_path.exists():
        print(f"Файл {file_path} не найден, пропускаем.")
        continue

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Обработка securities
    sec = data.get("securities")
    if sec and sec.get("data"):
        sec_df = pd.DataFrame(sec["data"], columns=sec["columns"])
        securities_dfs.append(sec_df)
    else:
        print(f"В файле {file_path} нет данных securities.")

    # Обработка marketdata
    mkt = data.get("marketdata")
    if mkt and mkt.get("data"):
        mkt_df = pd.DataFrame(mkt["data"], columns=mkt["columns"])
        marketdata_dfs.append(mkt_df)
    else:
        print(f"В файле {file_path} нет данных marketdata.")

# Объединение всех securities
if securities_dfs:
    all_securities = pd.concat(securities_dfs, ignore_index=True)
else:
    raise ValueError("Нет данных securities ни в одном файле.")

# Объединение всех marketdata
if marketdata_dfs:
    all_marketdata = pd.concat(marketdata_dfs, ignore_index=True)
else:
    # Если marketdata нет, создаем пустой DataFrame с колонкой SECID для join
    all_marketdata = pd.DataFrame(columns=["SECID"])

# Присоединяем marketdata к securities по SECID (left join)
df = pd.merge(all_securities, all_marketdata, on="SECID", how="left")

# Добавление столбца exclude (Исключить, по умолчанию False)
df["exclude"] = False

# Создаём маску: True, если в LATNAME есть "OFZ-PD"
mask = df["LATNAME"].str.contains("OFZ-PD", na=False)

# Разделяем
ofz_df = df[mask].copy()
corp_df = df[~mask].copy()

# Проверка размеров
print(f"OFZ bonds: {len(ofz_df)}")
print(f"Corporate bonds: {len(corp_df)}")


# Формирование условий для исключения corp_df
cond_yield = (corp_df["YIELD"] < bonds_yield_min) | (corp_df["YIELD"] > bonds_yield_max)
cond_price = (corp_df["PREVLEGALCLOSEPRICE"] < bonds_price_min) | (
    corp_df["PREVLEGALCLOSEPRICE"] > bonds_price_max
)
cond_duration = (corp_df["DURATION"] < bonds_duration_min) | (
    corp_df["DURATION"] > bonds_duration_max
)
cond_face = corp_df["FACEVALUE"] > 10000
cond_coupon = corp_df["COUPONPERIOD"] <= 0

# Применение условий corp_df – если любое истинно, ставим exclude = True
corp_df.loc[
    cond_yield | cond_price | cond_duration | cond_face | cond_coupon, "exclude"
] = True


# Формирование условий для исключения ofz_df
cond_yield = ofz_df["YIELD"] < risk_free_rate
cond_price = ofz_df["PREVLEGALCLOSEPRICE"] = 0
cond_face = ofz_df["FACEVALUE"] > 10000
cond_coupon = ofz_df["COUPONPERIOD"] <= 0

# Применение условий ofz_df – если любое истинно, ставим exclude = True
ofz_df.loc[cond_yield | cond_price | cond_face | cond_coupon, "exclude"] = True


# Сохраняем результат в CSV (опционально)
ofz_df.to_csv(os.path.join(output_dir, "ofz_df.csv"), index=False, encoding="utf-8-sig")
corp_df.to_csv(
    os.path.join(output_dir, "corp_df.csv"), index=False, encoding="utf-8-sig"
)


bondsdata_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{}.json?iss.meta=off&iss.only=coupons"
# 4. Перебор строк датафрейма corp_df
for index, row in corp_df.iterrows():
    # Пропускаем исключённые облигации
    if row["exclude"]:
        continue

    secid = row["SECID"]
    url = bondsdata_url.format(secid)
    filename = os.path.join(output_dir, f"coupons{secid}.json")

    try:
        print(f"Загрузка данных для {secid}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Проверка на ошибки HTTP

        # Сохранение JSON-ответа в файл
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"Данные для {secid} сохранены в {filename}")

        # Небольшая задержка, чтобы не перегружать сервер
        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке данных для {secid}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка для {secid}: {e}")

# 4. Перебор строк датафрейма ofz_df
for index, row in ofz_df.iterrows():
    # Пропускаем исключённые облигации
    if row["exclude"]:
        continue

    secid = row["SECID"]
    url = bondsdata_url.format(secid)
    filename = os.path.join(output_dir, f"coupons{secid}.json")

    try:
        print(f"Загрузка данных для {secid}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Проверка на ошибки HTTP

        # Сохранение JSON-ответа в файл
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"Данные для {secid} сохранены в {filename}")

        # Небольшая задержка, чтобы не перегружать сервер
        time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке данных для {secid}: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка для {secid}: {e}")

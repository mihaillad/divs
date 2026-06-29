import requests
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# ====================== КОНФИГУРАЦИЯ ======================
CONFIG = {
    "bonds_yield_min": 21,
    "bonds_yield_max": 60,
    "bonds_price_min": 60,
    "bonds_price_max": 96,
    "bonds_duration_min": 6,
    "bonds_duration_max": 120,
    "risk_free_rate": 15,
    "max_age_seconds": 3600,
    "output_dir": Path(__file__).parent / "data",
    "only_SECID": "",   # для отладки: оставить только один SECID
}

# ====================== РАБОТА С ФАЙЛАМИ ======================
def ensure_dir(path):
    """Создаёт директорию, если её нет."""
    os.makedirs(path, exist_ok=True)

def is_file_valid(filepath, max_age):
    """Проверяет: файл существует, не пустой и не старше max_age (сек)."""
    if not os.path.exists(filepath):
        return False
    if os.path.getsize(filepath) == 0:
        return False
    age = time.time() - os.path.getmtime(filepath)
    if age > max_age:
        return False
    return True

def download_json(url, filepath, params=None):
    """Загружает JSON по URL и сохраняет в файл."""
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        # print(f"Файл {filepath} обновлён.")
        time.sleep(1)  # щадящий режим 1 сек
        return True
    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")
        return False

def get_file(url, filepath, params=None, max_age=3600):
    """Загружает файл, если он отсутствует, пустой или устарел."""
    if not is_file_valid(filepath, max_age):
        print(f"Загрузка {filepath}...")
        return download_json(url, filepath, params)
    # print(f"Файл {filepath} актуален.")
    return True

# ====================== ЗАГРУЗКА ДАННЫХ С МОСБИРЖИ ======================
def load_board_data(board_list, market_type, output_dir, max_age):
    """
    Загружает данные для указанных торговых досок.
    market_type: 'bonds' или 'shares'
    """
    if market_type == "bonds":
        base_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/{boardgroup}/securities.json"
    else:  # shares
        base_url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/{boardgroup}/securities.json"

    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,ACCRUEDINT,LATNAME",
        "marketdata.columns": "SECID,BOARDID,YIELD,DURATION,LCURRENTPRICE,LAST",
    }

    for board in board_list:
        url = base_url.format(boardgroup=board)
        file_name = output_dir / f"prices_{board}.json"
        get_file(url, file_name, params, max_age)

def load_bond_groups(board_groups, output_dir, max_age):
    """Загружает данные по группам облигаций (boardgroups) для основного DataFrame."""
    base_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boardgroups/{boardgroup}/securities.json"
    params = {
        "iss.meta": "off",
        "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,MATDATE,ACCRUEDINT,LATNAME",
        "marketdata.columns": "SECID,YIELD,DURATION",
    }

    for group in board_groups:
        url = base_url.format(boardgroup=group)
        file_name = output_dir / f"{group}.json"
        get_file(url, file_name, params, max_age)

# ====================== СБОРКА DATAFRAME ======================
def build_dataframe_from_files(board_groups, output_dir):
    """Читает JSON-файлы групп и объединяет в один DataFrame."""
    securities_list = []
    marketdata_list = []

    for group in board_groups:
        file_path = output_dir / f"{group}.json"
        if not file_path.exists():
            print(f"Файл {file_path} не найден, пропускаем.")
            continue

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Securities
        sec = data.get("securities")
        if sec and sec.get("data"):
            df_sec = pd.DataFrame(sec["data"], columns=sec["columns"])
            securities_list.append(df_sec)

        # Marketdata
        mkt = data.get("marketdata")
        if mkt and mkt.get("data"):
            df_mkt = pd.DataFrame(mkt["data"], columns=mkt["columns"])
            marketdata_list.append(df_mkt)

    if not securities_list:
        raise ValueError("Нет данных securities.")

    all_sec = pd.concat(securities_list, ignore_index=True)
    all_mkt = pd.concat(marketdata_list, ignore_index=True) if marketdata_list else pd.DataFrame(columns=["SECID"])

    df = pd.merge(all_sec, all_mkt, on="SECID", how="left")
    return df

# ====================== ФИЛЬТРАЦИЯ И РАЗДЕЛЕНИЕ ======================
def split_ofz_corp(df):
    """Разделяет на OFZ и корпоративные по LATNAME."""
    mask_ofz = df["LATNAME"].str.contains("OFZ-PD", na=False)
    ofz = df[mask_ofz].copy()
    corp = df[~mask_ofz].copy()
    print(f"OFZ: {len(ofz)}, Corporate: {len(corp)}")
    return ofz, corp

def apply_exclusion_corp(df, cfg):
    """Применяет правила исключения для корпоративных облигаций."""
    df = df.copy()
    cond_yield = (df["YIELD"] < cfg["bonds_yield_min"]) | (df["YIELD"] > cfg["bonds_yield_max"])
    cond_price = (df["PREVLEGALCLOSEPRICE"] < cfg["bonds_price_min"]) | (df["PREVLEGALCLOSEPRICE"] > cfg["bonds_price_max"])
    cond_duration = (df["DURATION"] / 30 < cfg["bonds_duration_min"]) | (df["DURATION"] / 30 > cfg["bonds_duration_max"])
    cond_face = df["FACEVALUE"] > 10000
    cond_coupon = df["COUPONPERIOD"] <= 0
    df["exclude"] = cond_yield | cond_price | cond_duration | cond_face | cond_coupon
    return df

def apply_exclusion_ofz(df, cfg):
    """Применяет правила исключения для OFZ."""
    df = df.copy()
    cond_yield = df["YIELD"] < cfg["risk_free_rate"]
    cond_price = df["PREVLEGALCLOSEPRICE"] == 0   # исправлено: сравнение, а не присваивание
    cond_face = df["FACEVALUE"] > 10000
    cond_coupon = df["COUPONPERIOD"] <= 0
    df["exclude"] = cond_yield | cond_price | cond_face | cond_coupon
    return df

def save_dataframes(ofz_df, corp_df, output_dir):
    """Сохраняет OFZ и корпоративные DataFrame в CSV."""
    ofz_df.to_csv(output_dir / "ofz_df.csv", index=False, encoding="utf-8-sig")
    corp_df.to_csv(output_dir / "corp_df.csv", index=False, encoding="utf-8-sig")
    print("ofz_df.csv и corp_df.csv сохранены.")

# ====================== ЗАГРУЗКА КУПОНОВ ======================
def load_coupons_for_df(df, output_dir, max_age):
    """Загружает купонные данные для всех неисключённых облигаций из DataFrame."""
    bonds_data_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{}.json?iss.meta=off&iss.only=coupons"
    for index, row in df.iterrows():
        if row["exclude"]:
            continue
        secid = row["SECID"]
        url = bonds_data_url.format(secid)
        file_name = output_dir / f"coupons{secid}.json"
        get_file(url, file_name, params=None, max_age=max_age)

def retry_empty_coupon_files(output_dir, max_age):
    """Повторно загружает файлы coupons*.json, имеющие нулевой размер."""
    bonds_data_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{}.json?iss.meta=off&iss.only=coupons"
    for filename in os.listdir(output_dir):
        if filename.startswith("coupons") and filename.endswith(".json"):
            filepath = output_dir / filename
            if os.path.getsize(filepath) == 0:
                secid = filename[7:-5]  # удаляем "coupons" и ".json"
                url = bonds_data_url.format(secid)
                print(f"Повторная загрузка {secid} (файл был пуст)")
                download_json(url, filepath, params=None)


# ====================== ЗАГРУЗКА ИСТОРИИ ТОРГОВ ======================
def load_history_for_df(df, output_dir, max_age):
    """Загружает купонные данные для всех неисключённых облигаций из DataFrame."""
    fromdate = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    bonds_data_url = "https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/{BOARDID}/securities/{secid}.json?iss.meta=off&iss.only=history&history.columns=SECID,TRADEDATE,VOLUME,NUMTRADES&limit=20&from={fromdate}"
    for index, row in df.iterrows():
        if row["exclude"]:
            continue
        secid = row["SECID"]
        BOARDID = row["BOARDID"]
        url = bonds_data_url.format(secid=secid,BOARDID=BOARDID,fromdate=fromdate)
        file_name = output_dir / f"history{secid}.json"
        get_file(url, file_name, params=None, max_age=max_age)

def retry_empty_history_files(output_dir, max_age):
    """Повторно загружает файлы coupons*.json, имеющие нулевой размер."""
    fromdate = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    bonds_data_url = "https://iss.moex.com/iss/history/engines/stock/markets/bonds/boards/{BOARDID}/securities/{secid}.json?iss.meta=off&iss.only=history&history.columns=SECID,TRADEDATE,VOLUME,NUMTRADES&limit=20&from={fromdate}"
    for filename in os.listdir(output_dir):
        if filename.startswith("history") and filename.endswith(".json"):
            filepath = output_dir / filename
            if os.path.getsize(filepath) == 0:
                secid = filename[7:-5]  # удаляем "history" и ".json"
                url = bonds_data_url.format(secid=secid,fromdate=fromdate)
                print(f"Повторная загрузка {secid} (файл был пуст)")
                download_json(url, filepath, params=None)                

# ====================== ОСНОВНАЯ ФУНКЦИЯ ======================
def main():
    # Настройка
    cfg = CONFIG
    output_dir = cfg["output_dir"]
    ensure_dir(output_dir)

    # 1. Загрузка данных по доскам (не используются в основном датафрейме, но оставлены)
    print("Загрузка данных по доскам (облигации)...")
    load_board_data(["TQIR", "TQCB", "TQOD", "TQOB"], "bonds", output_dir, cfg["max_age_seconds"])
    print("Загрузка данных по доскам (акции)...")
    load_board_data(["TQTF", "TQBR"], "shares", output_dir, cfg["max_age_seconds"])

    # 2. Загрузка групп облигаций для основного датафрейма
    board_groups = [58, 105, 245]
    print("Загрузка групп облигаций...")
    load_bond_groups(board_groups, output_dir, cfg["max_age_seconds"])

    # 3. Сборка DataFrame
    df = build_dataframe_from_files(board_groups, output_dir)

    # Отладка: оставляем только один SECID, если задан
    if cfg["only_SECID"]:
        df = df[df["SECID"] == cfg["only_SECID"]]
        print(f"Отладка: оставлен только SECID={cfg['only_SECID']}")

    # 4. Разделение на OFZ и корпоративные
    ofz_df, corp_df = split_ofz_corp(df)

    # 5. Применение исключений
    corp_df = apply_exclusion_corp(corp_df, cfg)
    ofz_df = apply_exclusion_ofz(ofz_df, cfg)

    # 6. Сохранение CSV
    save_dataframes(ofz_df, corp_df, output_dir)

    max_age_seconds = cfg["max_age_seconds"]*24*7
   # 7. Загрузка купонов для корпоративных облигаций
    print("Загрузка купонов для корпоративных облигаций...")
    load_coupons_for_df(corp_df, output_dir, max_age_seconds)

    # 8. Загрузка купонов для OFZ
    print("Загрузка купонов для OFZ...")
    load_coupons_for_df(ofz_df, output_dir, max_age_seconds)

    # 9. Повторная загрузка пустых файлов купонов
    print("Проверка и повторная загрузка пустых купонных файлов...")
    retry_empty_coupon_files(output_dir, max_age_seconds)

    max_age_seconds = cfg["max_age_seconds"]*24
    # 7. Загрузка истории торгов для корпоративных облигаций
    print("Загрузка истории торгов для корпоративных облигаций...")
    load_history_for_df(corp_df, output_dir, max_age_seconds)

    # 8. Загрузка истории торгов для OFZ
    print("Загрузка истории торгов для OFZ...")
    load_history_for_df(ofz_df, output_dir, max_age_seconds)
    print("Скрипт завершён.")

    # 9. Повторная загрузка пустых файлов купонов
    # print("Проверка и повторная загрузка истории торгов...")
    # retry_empty_history_files(output_dir, max_age_seconds)

if __name__ == "__main__":
    main()
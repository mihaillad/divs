import requests
import json
import os
from pathlib import Path
import pandas as pd


base_url = "https://iss.moex.com/iss/engines/stock/markets/bonds/boardgroups/{boardgroup}/securities.json"
params = {
    "iss.meta": "off",
    "securities.columns": "SECID,BOARDID,FACEVALUE,PREVLEGALCLOSEPRICE,COUPONVALUE,COUPONPERIOD,COUPONPERCENT,NEXTCOUPON,MATDATE,ACCRUEDINT,LATNAME",
    "marketdata.columns": "SECID,YIELD,DURATION"
}

# Список интересующих площадок
boards = [58, 105, 207, 245]

# Директория для сохранения файлов
output_dir = r"F:\Documents\Деньги\Мосбиржа\bonds"
os.makedirs(output_dir, exist_ok=True)

# for board in boards:
#     url = base_url.format(boardgroup=board)
#     response = requests.get(url, params=params)
#     if response.status_code == 200:
#         data = response.json()
#         file_path = os.path.join(output_dir, f"{board}.json")
#         # Явно указываем кодировку UTF-8 для корректной записи кириллицы
#         with open(file_path, 'w', encoding='utf-8') as f:
#             json.dump(data, f, ensure_ascii=False, indent=4)
#         print(f"Данные по площадке {board} успешно сохранены в {file_path}")
#     else:
#         print(f"Ошибка при запросе данных для площадки {board}: код ответа {response.status_code}")


#Создаем датафрейм
        
# Списки для хранения DataFrame
securities_dfs = []
marketdata_dfs = []

for board in boards:
    file_path = Path(os.path.join(output_dir, f"{board}.json"))
    if not file_path.exists():
        print(f"Файл {file_path} не найден, пропускаем.")
        continue

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Обработка securities
    sec = data.get('securities')
    if sec and sec.get('data'):
        sec_df = pd.DataFrame(sec['data'], columns=sec['columns'])
        securities_dfs.append(sec_df)
    else:
        print(f"В файле {file_path} нет данных securities.")

    # Обработка marketdata
    mkt = data.get('marketdata')
    if mkt and mkt.get('data'):
        mkt_df = pd.DataFrame(mkt['data'], columns=mkt['columns'])
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
    all_marketdata = pd.DataFrame(columns=['SECID'])

# Присоединяем marketdata к securities по SECID (left join)
df = pd.merge(all_securities, all_marketdata, on='SECID', how='left')

# Создаём маску: True, если в LATNAME есть "OFZ-PD"
mask = df['LATNAME'].str.contains('OFZ-PD', na=False)

# Разделяем
ofz_df = df[mask].copy()
corp_df = df[~mask].copy()

# Проверка размеров
print(f"OFZ bonds: {len(ofz_df)}")
print(f"Corporate bonds: {len(corp_df)}")

# Сохраняем результат в CSV (опционально)
ofz_df.to_csv(os.path.join(output_dir, 'ofz_df.csv'), index=False, encoding='utf-8-sig')
corp_df.to_csv(os.path.join(output_dir, 'corp_df.csv'), index=False, encoding='utf-8-sig')


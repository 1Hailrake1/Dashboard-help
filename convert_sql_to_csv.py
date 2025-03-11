import asyncio
import datetime
import time

import aiohttp
import pyodbc
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import NVARCHAR
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import requests
import json
def schedule_daily_task():
    scheduler = AsyncIOScheduler()

    # Триггер для ежедневного запуска в 11:35
    trigger = CronTrigger(hour=12, minute=30)
    scheduler.add_job(set_and_update_data, trigger, misfire_grace_time=50)

    # Запуск планировщика
    scheduler.start()


async def update_links():
    try:
        print(f'Актуализация ссылок {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}')
        url = 'https://ru-edi.tedointernal.ru:8080/GetUrlOrFileWS.ashx?get=allbin&barcode='
        change_team_url = URL.create(
            "mssql+pyodbc",
            username="Change_Team_User",
            password="Moscow2024",
            host="RUMOWSQLPWV008",
            port="1433",
            database="Change_Team",
            query={"driver": "ODBC Driver 17 for SQL Server"},

        )
        change_team_engine = create_engine(change_team_url)
        query = '''
        			SELECT [Документ.Номер]
        			FROM 
        			dashboard_info.all_act_sign_control aasc 
        			'''

        documents = pd.read_sql(query, change_team_engine)
        new_data = []
        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            for index, row in documents.iterrows():
                try:
                    document = row['Документ.Номер']
                    destination_url = f'{url}{document}'
                    response = await session.get(url=destination_url)
                    if response.status != 200:
                        api_url = f'Error api request {response.status}'
                    elif 'File not found!' in await response.text():
                        api_url = 'File not found!'
                    else:
                        output_text = await response.text()
                        api_url = json.loads(output_text)
                        if len(api_url) == 2 and 'URL' in api_url[1]:
                            url1 = api_url[0]['URL']
                            url2 = api_url[1]['URL']
                            api_url = url1 if len(url1) > len(url2) else url2
                        else:
                            api_url = api_url[0]['URL']
                    new_data.append({
                        "id": index,
                        "URL": destination_url,
                        "Документ.Номер": document,
                        "API_Response.URL": api_url
                    })
                except Exception as E:
                    print(E)
        new_df = pd.DataFrame(new_data)
        new_df.to_sql('document_links', con=change_team_engine, if_exists='replace', index=False,
                      schema='dashboard_info')
        print('Скрипт актуализировал ссылки')
    except Exception as E:
        print(f'Актуализация ссылок провалена {E}')

async def set_and_update_data():
    try:
        print(f'Актуализация данных {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}')
        start_time = time.time()
        db_url =  URL.create(
                "mssql+pyodbc",
                username="UploadSDC",
                password="Con45_$Bbb",
                host="RUMOWSQLPWV053",
                port="1433",
                database="PowerBI_datamarts",
                query={"driver": "ODBC Driver 17 for SQL Server"}
        )
        change_team_url = URL.create(
                "mssql+pyodbc",
                username="Change_Team_User",
                password="Moscow2024",
                host="RUMOWSQLPWV008",
                port="1433",
                database="Change_Team",
                query={"driver": "ODBC Driver 17 for SQL Server"},

        )

        change_team_engine = create_engine(change_team_url)
        engine = create_engine(db_url)

        query = '''
        SELECT
        *
        FROM 
        PowerBI_datamarts.dbo.all_act_sign_control
        '''
        df = pd.read_sql(query, engine)
        dtype = {col:NVARCHAR(1000) for col in df.select_dtypes(include='object').columns}
        Session = sessionmaker(bind=change_team_engine)
        session = Session()
        df.to_sql('all_act_sign_control', con=change_team_engine, if_exists='replace', index=False,
                                schema='dashboard_info', dtype=dtype)
        session.close()
        await update_links()
        end_time = time.time()
        print('Скрипт актуализировал данные')
        print(f'Времы выполнения скрипта в секундах {end_time - start_time}')
    except Exception as E:
            print(f'Ошибка {E}')



async def main():
    """Основная асинхронная функция."""
    print(f'Скрипт запущен {datetime.datetime.now().strftime("%d-%m-%Y %H:%M")}')
    schedule_daily_task()
    # Бесконечный цикл для поддержания работы скрипта
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
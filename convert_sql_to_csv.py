import datetime
import time
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import NVARCHAR
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

def schedule_daily_task():
    scheduler = BackgroundScheduler()
    # Триггер для ежедневного запуска в 8:50
    trigger = CronTrigger(hour=11, minute=35)
    scheduler.add_job(set_and_update_data, trigger, misfire_grace_time=50)
    scheduler.start()

def clear_tables():
    change_url = URL.create(
        "mssql+pyodbc",
        username="Change_Team_User",
        password="Moscow2024",
        host="RUMOWSQLPWV008",
        port="1433",
        database="Change_Team",
        query={"driver": "ODBC Driver 17 for SQL Server"}
    )
    change_engine = create_engine(change_url)
    with change_engine.connect() as conn:
        querry1 = f'''DROP TABLE IF EXISTS dashboard_info.all_act_sign_control'''
        conn.execute(text(querry1))
        conn.commit()


def set_and_update_data():
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
            end_time = time.time()
            print('Скрипт актуализировал данные')
            print(f'Времы выполнения скрипта в секундах {end_time - start_time}')
        except Exception as E:
                print(f'Ошибка {E}')



if __name__ == "__main__":
    print(f'Скрипт запущен {datetime.datetime.now().strftime('%d-%m-%Y %H:%M')}')
    schedule_daily_task()
    while True:
        time.sleep(1)
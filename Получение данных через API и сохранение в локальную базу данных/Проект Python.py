"""
Задача для проекта - написать скрипт, в котором реализовано:
- обращение к API для получения данных;
- обработка и подготовка данных к загрузке в базу данных;
- загрузка обработанных данных в локальную базу PostgreSQL;
- сохранение во время обработки логов работы скрипта с отлавливанием всех ошибок и выводом промежуточных стадий. 
Файл именуется в соответствии с текущей датой. Если в папке с логами уже есть другие логи - они удаляются, остаются 
только логи за последние 3 дня.
В конце агрегированые данные за день (например, сколько попыток было совершено) выгружаются в таблицу Google Sheets.

"""


# Импорт
import requests
import psycopg2
import logging
from datetime import datetime, timedelta
import os
import gspread
import gspread_formatting
import smtplib
import ssl
import json
from email.message import EmailMessage
from ..params import HOST, PORT, DATABASE, USER, PASSWORD


# Настройка логов
log_file = 'logs.txt'
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s: %(message)s",
                    level=logging.INFO,
                    handlers=[logging.FileHandler(
                        log_file, 'a', 'utf-8')]
                    )


def clean_old_logs(log_file):
    """
    Функция для удаления старых записей из файла лога

    """
    # Определяем время, соответствующее 3 дням назад
    cutoff_time = datetime.now() - timedelta(days=3)
    new_lines = []

    # Читаем существующий лог файл
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            for line in file:
                log_time_str = line.split(' ')
                log_time_str = log_time_str[0] + ' ' + log_time_str[1]
                log_time = datetime.strptime(
                    log_time_str, '%Y-%m-%d %H:%M:%S,%f')
                # Если запись не старее 3 дней, добавляем её в новый список
                if log_time >= cutoff_time:
                    new_lines.append(line)
    # Перезаписываем логи
    with open(log_file, 'w') as file:
        file.writelines(new_lines)


class DatabaseOperations:
    # Подключение к БД
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                host=HOST,
                port=PORT
            )
            self.cur = self.conn.cursor()
            logging.info("Подключение к БД выполнено")
        except Exception:
            logging.info("Подключение к БД не выполнено. Произошла ошибка")

    # Создание таблицы Users в БД
    def create_table(self):
        try:
            self.cur.execute("""CREATE TABLE if not exists Users(
                        user_id TEXT,
                        oauth_consumer_key TEXT,
                        lis_result_sourcedid TEXT,
                        lis_outcome_service_url TEXT,
                        is_correct TEXT,
                        attempt_type TEXT,
                        created_at timestamp);
                            """)
            self.conn.commit()
            logging.info("Таблица в БД успешно создана")
        except Exception:
            logging.info("При создании таблицы в БД возникла ошибка")

    # Предобработка данных для дальнейшей загрузки
    def preparation_data(self):
        try:
            logging.info("Получение данных по API")

            # Получение параметров из файла
            with open('params.json', 'r') as f:
                dict_params = json.loads(f.read())
            self.params = dict_params
            self.api_url = "https://b2b.itresume.ru/api/statistics"
            response = requests.get(self.api_url, params=self.params)
            r = response.json()
            logging.info("Данные по API успешно получены")
            logging.info("Начало предобработки данных")

            self.res = []
            for i in r:
                info = {}
                info = {
                    'user_id': i['lti_user_id'],
                    'oauth_consumer_key': eval(i['passback_params']).get('oauth_consumer_key', ''),
                    'lis_result_sourcedid': eval(i['passback_params']).get('lis_result_sourcedid', ''),
                    'lis_outcome_service_url': eval(i['passback_params']).get('lis_outcome_service_url', ''),
                    'is_correct': i['is_correct'],
                    'attempt_type': i['attempt_type'],
                    'created_at': i['created_at']}
                self.res.append(info)
            logging.info("Данные успешно предобработаны")
        except Exception:
            logging.info("В ходе предобработки данных возникла ошибка")

    # Загрузка предобработанных данных в БД
    def loadind_to_db(self):
        logging.info("Начало загрузки данных в БД")
        try:
            for item in self.res:
                self.cur.execute(
                    """INSERT INTO Users (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, 
                is_correct, attempt_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s);""", (item['user_id'], item['oauth_consumer_key'],
                                                          item['lis_result_sourcedid'], item['lis_outcome_service_url'],
                                                          item['is_correct'], item['attempt_type'], item['created_at']))
                self.conn.commit()
            logging.info("Загрузка данных в БД успешно завершена")
        except Exception:
            logging.info("В ходе загрузки данных в БД возникла ошибка")

    # Закрытие подключения
    def close_connection(self):
        self.cur.close()
        self.conn.close()
        logging.info("Подключение закрыто")

    # Подсчет статистики
    def statistic(self):
        self.attempts = 0
        self.success = 0
        self.users = []
        self.time_of_day = {
            'morning': 0,
            'afternoon': 0,
            'evening': 0
        }
        for i in self.res:
            # Подсчет количества всех попыток (attempts)
            if i['attempt_type'] == 'submit':
                self.attempts += 1

            # Подсчет количества количества успешных попыток (success)
            if i['is_correct'] == 1:
                self.success += 1

            # Подсчет количества уникальных пользователей (users) за период
            if i['user_id'] not in self.users:
                self.users.append(i['user_id'])

            # Подсчет количества попыток за разное время суток
            if i['attempt_type'] == 'submit' and 4 <= datetime.strptime(i['created_at'], '%Y-%m-%d %H:%M:%S.%f').hour < 12:
                self.time_of_day['morning'] += 1
            if i['attempt_type'] == 'submit' and 12 <= datetime.strptime(i['created_at'], '%Y-%m-%d %H:%M:%S.%f').hour < 18:
                self.time_of_day['afternoon'] += 1
            if i['attempt_type'] == 'submit' and 18 <= datetime.strptime(i['created_at'], '%Y-%m-%d %H:%M:%S.%f').hour <= 23:
                self.time_of_day['evening'] += 1
        self.count_users = len(self.users)

    # Выгрузка отчета в GoogleSheets
    def load_report_to_sheets(self):
        try:
            gc = gspread.service_account(filename='creds.json')
            worksheet = gc.open("Report").sheet1
            worksheet.format("A1:A6", {
                "backgroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "horizontalAlignment": "LEFT",
                "textFormat": {
                    "foregroundColor": {
                        "red": 0.0,
                        "green": 0.0,
                        "blue": 0.0
                    },
                    "fontSize": 12,
                    "bold": False
                }
            })
            worksheet.format("B1:B6", {
                "backgroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "horizontalAlignment": "CENTER",
                "textFormat": {
                    "foregroundColor": {
                        "red": 0.0,
                        "green": 0.0,
                        "blue": 0.0
                    },
                    "fontSize": 12,
                    "bold": False
                }
            })
            gspread_formatting.set_column_widths(
                worksheet, [('A', 400), ('B:', 100)])
            worksheet.update([['Количество попыток', self.attempts],
                              ['Количество успешных попыток', self.success],
                              ['Количество уникальных пользователей',
                               self.count_users],
                              ['Количество попыток, выполненных утром',
                               self.time_of_day['morning']],
                              ['Количество попыток, выполненных днем',
                               self.time_of_day['afternoon']],
                              ['Количество попыток, выполненных вечером',
                               self.time_of_day['evening']]
                              ])
            logging.info("Выгрузка в GoogleSheets успешно выполнена")
        except Exception:
            logging.info(
                "Выгрузка в GoogleSheets не выполнена. Произошла ошибка")


a = DatabaseOperations()
a.create_table()
a.preparation_data()
a.loadind_to_db()
a.close_connection()
a.statistic()
a.load_report_to_sheets()

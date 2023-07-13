import os
import pandas as pd
from sqlalchemy import create_engine, MetaData,Table, Column, Integer, String, Float, Date


# Функция для загрузки файлов Excel в базу данных
def load_excel_files_to_db(folder_path, db_connection_string):
    # Создаем подключение к базе данных
    engine = create_engine(db_connection_string, encoding='utf8')
#     metadata =MetaData( schema='public', quote_schema=True)
 
#     # Удалите все таблицы из базы данных
#     metadata.drop_all(bind=engine)

#     loans_table = Table('Loans', metadata,
#                     Column('id', Integer, primary_key=True),
#                     Column('ИИН', String),
#                     Column('Остаток задолженности', Float),
#                     Column('Ставка', Float),
#                     Column('Дата_выдачи', Date),
#                     Column('Выделенная сумма', Float)
#                 )

# # Создаем таблицу, если она не существует
#     loans_table.create(checkfirst=True, bind=engine)

    # Получаем список файлов в папке
    file_list = os.listdir(folder_path)

    # Проходимся по каждому файлу и загружаем его в базу данных
    for file_name in file_list:
        if file_name.endswith('.xlsx'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_excel(file_path)

            # Загружаем данные в базу данных
            df.to_sql('bank2', engine, if_exists='append', index=False)

    print("Все файлы загружены в базу данных.")

# Функция для распределения задолженности по субъектам
def distribute_debt(db_connection_string):
    # Создаем подключение к базе данных
    engine = create_engine(db_connection_string)

    # Читаем данные из базы данных в DataFrame
    df = pd.read_sql_table('bank2', engine)

    # Выполняем расчет сумм задолженности по субъектам
    df['TotalDebt'] = df['Сумма предстоящих платежей/Остаточная сумма (основной долг), тенге'] + \
                      df['Сумма предстоящих платежей/Остаточная сумма (вознаграждение), тенге'] + \
                      df['Сумма просроченных платежей (основной долг), тенге'] + \
                      df['Сумма просроченных платежей (вознаграждение), тенге']

    # Группируем данные по субъектам (ИИН) и суммируем задолженность
    grouped_df = df.groupby('ИИН Заемщика')['TotalDebt'].sum().reset_index()

    # Функция для распределения задолженности по субъектам
        # Функция для распределения задолженности по субъектам
    # def distribute(row):
    #     if row['TotalDebt'] < 3000000:
    #         # Выделить 300000 на займы в следующей приоритетности
    #         iin = row['ИИН Заемщика']

    #         # Предположим, что у вас есть таблица с займами "Loans" в базе данных

    #         # Запрос для выбора займа с наибольшей ставкой
    #         query_max_interest = f"""
    #             SELECT * FROM Loans
    #             WHERE ИИН = '{iin}'
    #             ORDER BY Ставка DESC
    #             LIMIT 1
    #         """
    #         max_interest_loan = pd.read_sql(query_max_interest, engine)

    #         if not max_interest_loan.empty:
    #             max_interest_amount = min(max_interest_loan['Остаток задолженности'], 300000)
    #             max_interest_loan['Выделенная сумма'] = max_interest_amount

    #             # Обновление записи о займе с выделенной суммой
    #             max_interest_loan.to_sql('Loans', engine, if_exists='replace', index=False)

    #             # Уменьшение остатка задолженности
    #             row['TotalDebt'] -= max_interest_amount

    #         # Запрос для выбора займа с наименьшей датой выдачи
    #         query_min_date = f"""
    #             SELECT * FROM Loans
    #             WHERE ИИН = '{iin}'
    #             ORDER BY Дата_выдачи ASC
    #             LIMIT 1
    #         """
    #         min_date_loan = pd.read_sql(query_min_date, engine)

    #         if not min_date_loan.empty:
    #             min_date_amount = min(min_date_loan['Остаток задолженности'], row['TotalDebt'])
    #             min_date_loan['Выделенная сумма'] = min_date_amount

    #             # Обновление записи о займе с выделенной суммой
    #             min_date_loan.to_sql('Loans', engine, if_exists='replace', index=False)

    #             # Уменьшение остатка задолженности
    #             row['TotalDebt'] -= min_date_amount

    #     return row

    # Применяем функцию распределения задолженности
    grouped_df = grouped_df.apply(distribute, axis=1)


    # Применяем функцию распределения задолженности
    grouped_df.apply(distribute, axis=1)

    # Экспортируем итоговую таблицу в файл JSON
    grouped_df.to_json('output.json', orient='records')

    print("Распределение задолженности выполнено. Результат сохранен в output.json.")

# Путь к папке с файлами Excel
folder_path = r'\bank'

# Строка подключения к базе данных (вместо placeholders вставьте свои значения)
db_connection_string = 'postgresql://postgres:kaisar05@localhost:5432/postgres'


# Загрузить файлы Excel в базу данных
load_excel_files_to_db(folder_path, db_connection_string)

# Распределить задолженность по субъектам
distribute_debt(db_connection_string)

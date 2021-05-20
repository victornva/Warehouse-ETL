'''
Урок 5 - ДЗ

Не так давно мне понадобилось сделать практическую задачу: реализовать биллинг для сети телефонных серверов (asterisk).
Первым делом нужно было собрать данные с каждого сервера и положить в общую базу в едином формате - т.е. классический ETL.
Поэтому я так подумал, что в качестве ДЗ лучше подойдёт кусок реального модуля (он и сейчас работает), хотя проверить его работу к сожалению не получится т.к. работает он в закрытой сети.
Ниже привожу кусок кода который подключается к базе сервера MySQL (источник), считывает свежие данные (CDR) с учётом набора критериев, затем подключается к базе PostgresSQL (хранилище) и записывает туда полученные данные. Считанные в источнике данные в конце метятся как прочитанные (так надо по условиям задачки).
'''

import mysql.connector
from mysql.connector import Error
import psycopg2
from psycopg2 import OperationalError
#import string, datetime

#---------------------------------------------------------------------------------------------------------------

def pg_create_connection(db_host, db_port, db_user, db_password, db_name):
    ''' Ф-ция соединения с БД на сервере PostgreSQL'''
    connection = None
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        print("Connection to PostgreSQL DB successful!!")
    except OperationalError as e:
        print(f"The error '{e}' occurred...")
    return connection

#---------------------------------------------------------------------------------------------------------------

def mysql_create_connection(host_name, user_name, user_password, db_name):
    ''' Ф-ция соединения с БД на сервере MySQL'''
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

#---------------------------------------------------------------------------------------------------------------

def mysql_execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

#---------------------------------------------------------------------------------------------------------------

def mysql_execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

#---------------------------------------------------------------------------------------------------------------
# Создаём соединение с БД MySQL
mysql_connection = mysql_create_connection("192.168.11.9", "cdruser", "cdrpassword", "cdrdb")

print('Модуль переноса CDR')
# формируем выборку
select_cdr = "SELECT calldate, channel, dstchannel, src, dst, dcontext, duration, billsec " + \
             "FROM cdr " + \
             "WHERE billsec > 0 " + \
             "AND  calldate > '2020-09-01' " + \
             "AND  marked = 0 " + \
             "LIMIT 10"

# выполняем запрос
calls = mysql_execute_read_query(mysql_connection, select_cdr)

#---------------------------------------------------------------------------------------------------------------
# Создаём соединение с БД PostgreSQL
pg_connection = pg_create_connection("192.168.51.120", "5432", "billadmin", "billadminpwd", "billdb")
pg_connection.autocommit = True

# считаем сколько записей в выборке и формируем INSERT
calls_number = ", ".join(["%s"] * len(calls))

insert_paidcalls = (
    f"INSERT INTO paidcalls (calldate, channel, dstchannel, src, dst, dcontext, duration, billsec) VALUES {calls_number}")
# создаём курсор и выполняем запрос
cursor = pg_connection.cursor()
cursor.execute(insert_paidcalls, calls)

#----------------------------------------------------------------------------------------------
# метим скопированные записи дабы исключить их повторное копирование
mark_calls = '''UPDATE cdr 
                SET marked = 1 
                WHERE billsec > 0
                AND  calldate > '2020-09-01' 
                AND  marked = 0 
                LIMIT 10
'''
mysql_execute_query(mysql_connection, mark_calls)

#----------------------------------------------------------------------------------------------
mysql_connection.close()
pg_connection.close()

import sqlite3
import os.path as ospath
import psycopg2 as psql

#sqlite_db_path = '/home/giovanni/Documents/TrabalhoTopicoEspeciais/backend-fog/app/database/projetoTER.db'
sqlite_db_path = 'C:\\Users\\Giovanni Pereira\\Desktop\\PastaCompartilhadaRasp\\fudido\\TrabalhoTopicoEspeciais\\backend-fog\\app\\database\\projetoTER.db'
try:
    conn_sqlite = sqlite3.connect(sqlite_db_path, check_same_thread=False)
except Exception as e:
    print(e)
    exit(0)
cursor_sqlite = conn_sqlite.cursor()

try:
    conn_psql = psql.connect(
        host='ec2-52-205-61-230.compute-1.amazonaws.com',
        database='d6j3vgaift8ile',
        user='zqiusuicrcxeqj',
        password='de6e180d4624309bf13df7595a769267cf6289da3441a54c9f08531dadf12dd2',
        port=5432
    )
except Exception as e:
    print(e)
    exit(0)

cursor_postgres = conn_psql.cursor()
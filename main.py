from threading import Thread
from time import sleep
from database.db import conn_psql, conn_sqlite
import paho.mqtt.client as mqtt
import rsa
import ast

TYPES = {"<class 'int'>": 'int', "<class 'double'>": 'double', "<class 'bool'>": 'boolean'}
print(TYPES)
def as_tipo_dado(m):
    k = str(type(ast.literal_eval(m)))
    return TYPES[k] if k in TYPES.keys() else None

def get_all_topicos_from_db():
    sqlite_cursor = conn_sqlite.cursor()
    sqlite_cursor.execute('SELECT topico_mqtt FROM sensor;')
    return sqlite_cursor.fetchall()

def handle_message(topico: str, msg: str):
    sqlite_cursor = conn_sqlite.cursor()
    psql_cursor = conn_psql.cursor()
    try:
        sqlite_cursor.execute('''SELECT A.id, A.chave_publica, B.tipo FROM sensor as A, tipo_sensor as B WHERE A.topico_mqtt = ? AND A.id_tipo_sensor = B.id;''', [topico])
        id_sensor, chave_publica, tipo_sensor = sqlite_cursor.fetchone()

        sqlite_cursor.execute('''SELECT tipo FROM leitura_has_sensor AS a, tipo_leitura AS b WHERE a.id_sensor = ? AND b.id = a.id_tipo_leitura;''', [id_sensor])
        tipo_leitura, = sqlite_cursor.fetchone()
        chave_publicaPK = rsa.PublicKey.load_pkcs1(chave_publica)
        tipo_dado = as_tipo_dado(msg)
        if tipo_dado is not None:
            encrypt_msg = rsa.encrypt(msg.encode('utf-8'), chave_publicaPK)
            
            psql_cursor.execute('SELECT idsensor FROM sensor WHERE sensor.id_from_fog = %s', (id_sensor,))
            id_sensor_aux = psql_cursor.fetchone()
            if id_sensor_aux is None:
                psql_cursor.execute('INSERT INTO sensor(tiposensor, id_from_fog) VALUES (%s,%s) RETURNING idsensor;', (tipo_sensor, id_sensor))
                id_sensor, = psql_cursor.fetchone()
                psql_cursor.execute('INSERT INTO dado( dado, tipodado, tipoleitura, idsensor) VALUES (%s,%s,%s,%s)', (encrypt_msg, tipo_dado, tipo_leitura, id_sensor))
            else:
                psql_cursor.execute('INSERT INTO dado( dado, tipodado, tipoleitura, idsensor) VALUES (%s,%s,%s,%s)', (encrypt_msg, tipo_dado, tipo_leitura, id_sensor_aux[0]))
            conn_psql.commit()
        else:
            print('Tipo de dado nao existe')
    except Exception as e:
        print(e, type(e))
        conn_psql.rollback()
    
def create_mqtt_client():
    client = mqtt.Client()
    #Connection success callback
    def on_connect(client, userdata, flags, rc):
        print('Connected with result code ' + str(rc))

    # Message receiving callback
    def on_message(client, userdata, msg: mqtt.MQTTMessage):
        handle_message(msg.topic, msg.payload.decode('utf-8'))
    # Specify callback function
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set("giovanni", "geladeira0")
    
    # Establish a connection
    client.connect('127.0.0.1', 1883)
   
    return client

def subcribe_all_topics(client: mqtt.Client):
    #print('init thread',client)
    print(client.is_connected())
    while client.is_connected():
        topicos = get_all_topicos_from_db()
        for topico in topicos:
            t, = topico # unpack da tupla
            client.subscribe(t)
        sleep(2)

if __name__ == '__main__':
    client = create_mqtt_client()
    t1 = Thread(target=subcribe_all_topics,args=(client,))
    client.loop_start()
    t1.start()
    t1.join()
    client.loop_stop()
    client.loop_forever()
from database.db import conn_psql, conn_sqlite
import paho.mqtt.client as mqtt
import rsa
import ast

TYPES = {type(int): 'int', type(float): 'double', type(bool): 'boolean'}
def get_tipo_dado(m):
    return TYPES[ast.literal_eval(m)] if ast.literal_eval(m) in TYPES.keys() else None

def get_all_topicos_from_db():
    sqlite_cursor = conn_sqlite.cursor()
    sqlite_cursor.execute('SELECT topico_mqtt FROM sensor;')
    return sqlite_cursor.fetchall()

def handle_message(topico, msg):
    sqlite_cursor = conn_sqlite.cursor()
    psql_cursor = conn_psql.cursor()
    try:
        sqlite_cursor.execute('SELECT A.id, A.chave_publica, B.tipo FROM sensor as A, tipo_sensor as B WHERE A.topico_mqtt = %s AND A.id_tipo_sensor = B.id;', (topico))
        id_sensor, chave_publica, tipo_sensor = sqlite_cursor.fetchone()

        sqlite_cursor.execute('SELECT tipo FROM leitura_has_sensor AS a, tipo_leitura AS b WHERE a.id_sensor = %s AND b.id = a.id_tipo_leitura;', (id_sensor))
        tipo_leitura, = sqlite_cursor.fetchone()
        chave_publicaPK = rsa.PublicKey.load_pkcs1(bytes(chave_publica))
        tipo_dado = get_tipo_dado(msg)
        if tipo_dado is not None:
            encrypt_msg = rsa.encrypt(msg, chave_publicaPK)
            psql_cursor.execute('INSERT INTO sensor(tiposensor) VALUES (?) RETURNING id;', (tipo_sensor))
            id_sensor, = psql_cursor.fetchone()
            psql_cursor.execute('INSERT INTO dado( dado, tipodado, tipoleitura, idsensor) VALUES (?,?,?,?)', (encrypt_msg, tipo_dado, tipo_leitura, id_sensor))
            conn_psql.commit()
    except Exception as e:
        print(e)
        conn_psql.rollback()
    

def create_mqtt_client():
    client = mqtt.Client()
    #Connection success callback
    def on_connect(client, userdata, flags, rc):
        print('Connected with result code ' + str(rc))

    # Message receiving callback
    def on_message(client, userdata, msg: mqtt.MQTTMessage):
        print(msg.topic+" " + str(msg.payload))
        handle_message(msg.topic, msg.payload.decode('utf-8'))
    # Specify callback function
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set("giovanni", "geladeira0")

    # Establish a connection
    client.connect('127.0.0.1', 1883)

    topicos = get_all_topicos_from_db()
    for topico in topicos:
        t, = topico # unpack da tupla
        client.subscribe(t)

    return client

if __name__ == '__main__':
    client = create_mqtt_client()
    client.loop_forever()
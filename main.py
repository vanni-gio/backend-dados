from database.db import cursor_sqlite, cursor_postgres
import paho.mqtt.client as mqtt
import rsa

def get_all_topicos_from_db():
    cursor_sqlite.execute('SELECT topico_mqtt FROM sensor;')
    return cursor_sqlite.fetchall()

def handle_message(topico, msg):
    # receber os dados, verificiar a quem pertence os dados, inserir na cloud os dados
    cursor_sqlite.execute('SELECT chave_publica, id_tipo_leitura FROM sensor WHERE sensor.mqtt_topic = %s;', (topico))
    chave_publica, id_tipo_leitura = cursor_sqlite.fetchone()
    cursor_sqlite.execute('SELECT tipo FROM tipo_leitura WHERE tipo_leitura.id = %s;', (id_tipo_leitura))
    tipo_leitura = cursor_sqlite.fetchone()
    chave_publicaPK = rsa.PublicKey.load_pkcs1(bytes(chave_publica))
    encrypt_msg = rsa.encrypt(msg, chave_publicaPK)
    cursor_postgres.execute('INSERT INTO sensor')
    pass


def create_mqtt_client():
    client = mqtt.Client()
    #Connection success callback
    def on_connect(client, userdata, flags, rc):
        print('Connected with result code ' + str(rc))

    # Message receiving callback
    def on_message(client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))
        handle_message(msg.topic, str(msg.payload))
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
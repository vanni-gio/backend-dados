from flask import Flask, jsonify
from database.db import cursor_postgres


app = Flask(__name__)

@app.route('/dados')
def dados():
    cursor_postgres.execute('SELECT dado, tipodado, tipoleitura, idsensor FROM dado;')
    dados = cursor_postgres.fetchall()
    resp = {"sensores": []}
    for data in dados:
        dado, tipodado, tipoleitura, idsensor = data
        json_data = {"id" : idsensor, "data": dado, "tipodado": tipodado, "tipoleitura": tipoleitura}
        resp["sensores"].append(json_data)
    
    return jsonify(resp), 200
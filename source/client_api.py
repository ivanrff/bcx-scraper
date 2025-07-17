from flask import Flask, request, jsonify
from pymongo import MongoClient
# from kafka import KafkaProducer
import json
from scraper import scraper
from utils import mongo_to_json


app = Flask(__name__)

client = MongoClient("mongodb://admin:admin@localhost:27017/")

# criando a base de dados
db = client["bcx_db"]

# criando coleções
clients_collection = db["clients"]
pending_ce_collection = db["pending_ce"]
bls_collection = db["bls"]

# # criando producer
# producer = KafkaProducer(
#     bootstrap_servers='kafka:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

## REGISTRAR (CLIENT, BL)
## EX.: http://localhost:5000/check_bl?client_id=1&bl=99999999999999123
@app.route('/check_bl', methods=['GET'])
def check_bl():
    client_id = request.args.get("client_id")
    bl_number = request.args.get("bl")

    # checando se foram inseridos os client_id e bl no url
    if not client_id or not bl_number:
        return jsonify({"erro": "Parâmetros 'client_id' e 'bl' são obrigatórios"}), 400

    # buscar o client_id na collection clients
    client = clients_collection.find_one({"client_id": client_id})

    # se o client_id existe na collection clients
    if client:

        # buscar o registro do bl_number na collection bls
        bl_full = bls_collection.find_one({"origem_destino_carga.bl_conhecimento_embarque_original": bl_number})
        
        bl_full_json = mongo_to_json(bl_full)

        # se o registro deste BL existe na collection bls
        if bl_full_json:

            # se o client_id já está rastreando esse bl_number,
            if bl_number in client.get("bls", []): 
                # return jsonify({"mensagem": f"BL {bl_number} já registrado para o client {client_id}"}), 200
                return jsonify({"mensagem": f"BL {bl_number} já registrado para o client {client_id}", "BL": bl_full_json}), 200

            
            # se o client_id não está rastreando esse bl_number,
            else: 
                clients_collection.update_one(
                    {"client_id": client_id},
                    {"$push": {"bls": bl_number}}
                )
                # return jsonify({"mensagem": f"BL {bl_number} adicionado para o client {client_id}"}), 201
                return jsonify({"mensagem": f"BL {bl_number} adicionado para o client {client_id}", "BL": bl_full_json}), 201
        
        # se o registro deste BL não existe na collection bls, ativar o scrapper
        else:
            # inserir o kafka
            # kafka para ativar o scrapper
            # producer.send("scrapper-find-bl", {"bl": bl_number})
            # return jsonify({"mensagem": f"BL enviado para o Scrapper"}), 202
            scraper_results = mongo_to_json(scraper(bl_number))

            if isinstance(scraper_results, str):
                return scraper_results
            else:

                if bl_number not in client.get("bls", []): 
                    clients_collection.update_one(
                        {"client_id": client_id},
                        {"$push": {"bls": bl_number}}
                    )
                return mongo_to_json(scraper(bl_number))
    
    else:
        # se o cliente não existe, registrar o cliente e o bl [PARA DESENVOLVIMENTO]
        clients_collection.insert_one({"client_id": client_id, "bls": [bl_number]})
        return jsonify({"mensagem": f"Cliente {client_id} criado com BL {bl_number}"}), 201
    
        # se o cliente não existe, ERRO [PARA PRODUCAO]
        # return jsonify({"erro": f"Cliente {client_id} não existe na base de dados"}), 400

if __name__ == '__main__':
    app.run(debug=True)
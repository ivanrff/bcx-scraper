from flask import Flask, request, jsonify, Response, render_template
from flask_cors import CORS
from pymongo import MongoClient
import time
import json
from utils import mongo_to_json
from kafka import KafkaProducer, KafkaConsumer

# Startando a porta do Flask e criando conexão com o mongo
app = Flask(__name__)
CORS(app)
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["bcx_db"]

# Instanciando as coleções do mongo
clients_collection = db["clients"]
pending_ce_collection = db["pending_ce"]
bls_collection = db["bls"]

# criando producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Renderizar o client.html
@app.route('/')
def serve_client():
    return render_template("client.html")


@app.route("/check_bl_sse", methods=["GET"])
def check_bl_sse():

    # coletar client_id e bl_number, vindo do html
    client_id = request.args.get("client_id")
    bl_number = request.args.get("bl")

    # garantir que os dois foram recebidos
    if not client_id or not bl_number:
        return jsonify({"erro": "Parâmetros 'client_id' e 'bl' são obrigatórios"}), 400

    def event_stream():
        yield sse_event({"mensagem": f"Iniciando verificação para BL {bl_number}..."})
        
        # busca em clients_collection se o client já existe
        client = clients_collection.find_one({"client_id": client_id})
        # busca em bls_collection se o bl já está registrado
        bl_full = bls_collection.find_one({"origem_destino_carga.bl_conhecimento_embarque_original": bl_number})

        if client:
            if bl_full:
                # remover o "_id" do mongo já que não é relevante aqui
                bl_full_json = mongo_to_json(bl_full)

                # se o client já está rastreando o número BL:
                if bl_number in client.get("bls", []):
                    yield sse_event({"mensagem": f"BL {bl_number} já registrado para o client {client_id}", "BL": bl_full_json, "finalizado": True})
                
                else:
                    # se o client não está rastreando o número BL, registrar
                    clients_collection.update_one({"client_id": client_id}, {"$push": {"bls": bl_number}})
                    yield sse_event({"mensagem": f"BL {bl_number} adicionado para o client {client_id}", "BL": bl_full_json, "finalizado": True})
            else:
                # se o BL não foi encontrado em bls_collection, ativar o scraper via Kafka
                yield sse_event({"mensagem": "BL não encontrado na base. Rastreando via scraper..."})
                time.sleep(1)  # simula delay
                # Produz mensagem no Kafka para worker processar
                producer.send("scrapper-find-bl", {"bl": bl_number, "client_id": client_id})
                producer.flush()

                # Agora escuta o tópico de retorno
                yield from event_stream_kafka(client_id, bl_number)

        # se não encontrou o client_id na coleção de clients, adicionar [Isso não deveria ocorrer em produção]        
        else:
            clients_collection.insert_one({"client_id": client_id, "bls": [bl_number]})
            yield sse_event({"mensagem": f"Cliente {client_id} criado com BL {bl_number}", "finalizado": True})

    return Response(event_stream(), content_type='text/event-stream')

def event_stream_kafka(client_id, bl_number):
    consumer = KafkaConsumer(
        "scrapper-bl-results",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=f"sse-group-{client_id}-{bl_number}", # só pegar as mensagens deste client_id e deste bl_number
        auto_offset_reset="latest",  # pega só as mensagens novas
        consumer_timeout_ms=10000    # tempo de espera caso não haja mensagem nova
    )

    yield sse_event({"mensagem": f"Escutando atualizações para BL {bl_number}..."})

    for message in consumer:
        data = message.value
        # Verifica se a mensagem é relevante para esse client/bl
        if data.get("bl") == bl_number:
            yield sse_event({"mensagem": f"Atualização do Kafka recebida para BL {bl_number}"})
            yield sse_event({"BL": data})
            yield sse_event({"finalizado": True})
            break

    consumer.close()


def sse_event(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
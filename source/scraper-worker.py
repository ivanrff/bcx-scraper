from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import json, os
from scraper import read_html_file, process_html_data
from utils import mongo_to_json
from datetime import datetime

# consome as mensagens produzidas pela API
consumer = KafkaConsumer(
    'scrapper-find-bl',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='scraper-worker-group',
    auto_offset_reset='earliest'
)

# produz as mensagens de volta
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mongo = MongoClient("mongodb://admin:admin@localhost:27017/")
db = mongo["bcx_db"]
bls_collection = db["bls"]
clients_collection = db["clients"]
pending_ce_collection = db["pending_ce"]

def scraper(current_bl_number, current_folder_path = "siscarga/html_gerados"):

    html_files = [f for f in os.listdir(current_folder_path) if f.endswith(('.html', '.htm'))]

    if not html_files:
        sem_html_na_pasta = f"[{datetime.now()}] Nenhum arquivo HTML (.html ou .htm) encontrado na pasta: {current_folder_path}"
        print(sem_html_na_pasta)
        return sem_html_na_pasta

    print(f"\n--- Iniciando processamento de {len(html_files)} arquivo(s) na pasta '{current_folder_path}' ---")

    for filename in html_files:
        full_filepath = os.path.join(current_folder_path, filename)
        print(f"\n[{datetime.now()}] Processando arquivo: {filename}")
        html_content = read_html_file(full_filepath)

        if html_content:
            # Usa o BL da rodada atual
            collected_data = process_html_data(html_content, current_bl_number, filename)
            if collected_data:
                break
    
    print(f"\n--- Finalizado processamento de {len(html_files)} arquivo(s) na pasta '{current_folder_path}' ---")

    # --- Log de Sucesso/Insucesso ---
    if collected_data: # Se a lista de dados coletados não estiver vazia

        bl_number = collected_data["json_final"]["origem_destino_carga"]["bl_conhecimento_embarque_original"]
        ce_number = collected_data["json_final"]["manifesto_conhecimento"]["n_ce"]
        
        # Em caso não haja numero de CE, inserir na collection de pending_CEs
        if ((ce_number == None) | (ce_number == "")):
            print(f"[{datetime.now()}] BL {bl_number} encontrado mas CE não encontrado.")
            
            # checa se o BL em questão já estava na base de dados de pending_CEs
            bl_in_pending_ce_collection = pending_ce_collection.find_one({"nr_bl": bl_number})
            
            # se não está, inserir (EVITAR DUPLICIDADE)
            if not bl_in_pending_ce_collection:
                pending_ce_collection.insert_one({"nr_bl": bl_number})
                print(f"[{datetime.now()}] BL {bl_number} inserido na coleção de CEs pendentes.")
            else:
                print(f"[{datetime.now()}] BL {bl_number} já estava na coleção de CEs pendentes.")

            # checa se o BL em questão já estava na base de dados de BLs
            bl_in_collection = bls_collection.find_one({"origem_destino_carga.bl_conhecimento_embarque_original": bl_number})

            # se  não está, inserir
            if not bl_in_collection:
                bls_collection.insert_one(collected_data["json_final"])
                print(f"[{datetime.now()}] BL {bl_number} inserido na coleção de BLs.")

            return collected_data['json_final']

        else:
            # buscar se o bl já havia sido registrado na base de dados
            old_bl_data = bls_collection.find_one({"origem_destino_carga.bl_conhecimento_embarque_original": bl_number})
            
            # se sim, atualizar tudo, reescrevendo tudo
            if old_bl_data:
                bls_collection.update_one({"origem_destino_carga.bl_conhecimento_embarque_original": bl_number}, {"$set": collected_data["json_final"]})
                print(f"[{datetime.now()}] BL {bl_number} atualizado na coleção de BLs.")
            # se não, cria nova entrada
            else:
                bls_collection.insert_one(collected_data["json_final"])
                print(f"[{datetime.now()}] BL {bl_number} inserido na coleção de BLs.")

            return collected_data['json_final']

    else:
        no_data = "Nenhum dado relevante encontrado nesta rodada de processamento."
        print(no_data)
        return no_data


# itera sobre todas as mensagens que o consumer capturou e pra cada mensagem, roda o scraper
for message in consumer:
    try:
        data = message.value
        bl_number = data.get("bl")
        client_id = data.get("client_id")

        if not bl_number or not client_id:
            print(f"[ERRO] Mensagem inválida recebida: {data}")
            continue

        print(f"[INFO] Rastreando BL {bl_number} para client {client_id}")
        scraped = scraper(bl_number)

        # se o scraper não retornou uma string, ou seja, retornou os dados coletados,
        if not isinstance(scraped, str):
            scraped.pop('_id', None)
            bls_collection.insert_one(scraped)
            clients_collection.update_one({"client_id": client_id}, {"$push": {"bls": bl_number}})
            
            # Enviar de volta
            producer.send("scrapper-bl-results", {
                "client_id": client_id,
                "bl": bl_number,
                "scraped": mongo_to_json(scraped)
            })
        else:
            print(f"[WARN] Scraper retornou erro: {scraped}")

    except Exception as e:
        print(f"[ERRO] Exceção no worker: {e}")


    producer.flush()

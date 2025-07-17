from pymongo import MongoClient
import random

client = MongoClient("mongodb://admin:admin@localhost:27017/")

# criando a base de dados
db = client["bcx_db"]

# criando coleções
clients_collection = db["clients"]
pending_ce_collection = db["pending_ce"]
bls_collection = db["bls"]

def clear_collections():
    pending_ce_collection.delete_many({})
    bls_collection.delete_many({})
    clients_collection.delete_many({})

def insert_raw_data():
    clients_collection.insert_one({"client_id": "1", "bls": ["99999999999999990", "99999999999999991", "99999999999999992", "99999999999999993", "99999999999999994", "99999999999999995"]})
    clients_collection.insert_one({"client_id": "2", "bls": ["99999999999999991"]})
    #clients_collection.insert_one({"client_id": 3, "bls": ["99999999999999994", "99999999999999997", "99999999999999996"]})
    #clients_collection.insert_one({"client_id": 4, "bls": ["99999999999999997", "99999999999999998"]})
    #clients_collection.insert_one({"client_id": 5, "bls": ["99999999999999999", "99999999999999995", "99999999999999990", "99999999999999994"]})
    

clear_collections()
insert_raw_data()
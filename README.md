# 1. Acionar o container Docker com Mongo e Mongo Express

(Precisa do Docker instalado no computador)

Certifique-se do ```arquivo docker-compose.yml``` na pasta e rode no console:

```
docker-compose up -d
```

O container fica então acessível na porta ```localhost:27017``` e o Mongo Express em http://localhost:8081.

# 2. Teste

Em uma janela do cmd, rodar:
```
python source/client_api_async.py
```
Em outra janela do cmd, rodar:
```
python source/scraper-worker.py
```
Acessar http://localhost:5000/

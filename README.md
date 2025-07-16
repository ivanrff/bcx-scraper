# 1. Acionar o container Docker com Mongo e Mongo Express

Certifique-se do ```arquivo docker-compose.yml``` na pasta e rode no console:

```
docker-compose up -d
```

O container fica então acessível na porta ```localhost:27017``` e o Mongo Express em http://localhost:8081.

# 2. Endpoint Flask para simular interação do cliente

```
python api_client.py
```

- **Endpoint 1:** Adicionar novo bl para rastreio para um cliente em específico.

Recebe um client_id e um bl e verifica se já existe na base de dados, se não existir, atualiza.

Ex. de uso: ```http://localhost:5000/registrar_bl?client_id=1&bl=99999999999999999```
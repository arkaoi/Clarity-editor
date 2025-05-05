#!/bin/bash


for i in {1001..10000}
do
    curl -X PUT -H "Content-Type: application/json" \
         -d "{\"value\": \"value${i}\"}" \
         http://localhost:8080/database/key${i}
    echo "Запрос $i отправлен"
done

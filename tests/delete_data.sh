#!/bin/bash


for i in {500..900}
do
    curl -X DELETE http://localhost:8080/database/key${i}
    echo "Ключ $i удален"
done
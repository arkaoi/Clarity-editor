#!/usr/bin/env bash

BASE_URL="http://localhost:8080/database"

all_ok=true

for i in $(seq 1 100); do
  KEY="keyTest$i"
  URL="$BASE_URL/$KEY"

  echo -n "Checking $KEY ... "

  response=$(curl -s -w "\n%{http_code}" "$URL")
  body=$(echo "$response" | head -n -1)
  status=$(echo "$response" | tail -n1)

  if [[ "$status" == "200" && -n "$body" && "$body" != "null" ]]; then
    echo "OK"
  else
    echo "FAILED (HTTP $status)"
    all_ok=false
  fi
done

if [ "$all_ok" = true ]; then
  echo "✅ Все 100 структур успешно найдены."
else
  echo "❌ Некоторые структуры не были найдены или вернули ошибку."
fi

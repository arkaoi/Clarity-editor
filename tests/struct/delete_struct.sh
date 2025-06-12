#!/usr/bin/env bash

BASE_URL="http://localhost:8080/database"

all_ok=true

for i in $(seq 10 100); do
  KEY="keyTest$i"
  URL="$BASE_URL/$KEY"

  echo -n "Deleting $KEY ... "

  status=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$URL")

  if [[ "$status" == "200" || "$status" == "204" ]]; then
    echo "OK"
  else
    echo "FAILED (HTTP $status)"
    all_ok=false
  fi
done

if [ "$all_ok" = true ]; then
  echo "Все 100 ключей успешно удалены."
else
  echo "Некоторые ключи не удалось удалить."
fi

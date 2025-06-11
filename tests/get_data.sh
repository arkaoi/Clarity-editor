#!/bin/bash

for i in {2000..10000}; do
  if curl -sf http://localhost:8080/database/key${i} >/dev/null; then
    # код 200…299
    echo
  else
    # curl не вернул 2xx (например, 404) — считаем, что ключа нет
    echo "NO"
  fi
done
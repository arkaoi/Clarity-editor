#!/bin/bash

for i in {2000..10000}; do
  if curl -sf http://localhost:8080/database/key${i} >/dev/null; then
    echo
  else
    echo "NO"
  fi
done
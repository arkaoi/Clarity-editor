#!/usr/bin/env bash

BASE_URL="http://localhost:8080/database"

generate_payload() {
  local i=$1
  cat <<EOF
{
  "value": {
    "meta": {
      "id": $i,
      "name": "user_$i",
      "active": $((i % 2 == 0)),
      "roles": ["user", "test$((i % 5))"],
      "created_at": "2025-06-11T12:00:$((i % 60))Z"
    },
    "settings": {
      "level": $((i % 10)),
      "features": {
        "experimental": $((i % 3 == 0)),
        "quota": null,
        "limits": {
          "storage": $((100 + i)),
          "requests": [$((i * 10)), $((i * 20))]
        }
      },
      "modes": ["fast", "safe", "turbo"]
    },
    "phases": [
      { "name": "start", "duration_days": $((i % 7 + 1)) },
      { "name": "middle", "duration_days": $((i % 14 + 1)) },
      { "name": "end", "duration_days": $((i % 3 + 1)) }
    ]
  }
}
EOF
}

for i in $(seq 1 100); do
  KEY="keyTest$i"
  echo "Sending structure #$i to $BASE_URL/$KEY"

  curl -s -o /dev/null -w "%{http_code}\n" \
    -X PUT -H "Content-Type: application/json" \
    -d "$(generate_payload "$i")" \
    "$BASE_URL/$KEY"
done

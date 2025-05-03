ENDPOINT="http://localhost:8080/database"
TOTAL_KEYS=993

for i in $(seq 1 $TOTAL_KEYS); do
    curl -X PUT -H "Content-Type: application/json" \
        -d "{\"value\": \"value$i\"}" \
        "$ENDPOINT/key$i" &
done

wait
echo "Все $TOTAL_KEYS ключей отправлены!"

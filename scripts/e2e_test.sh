#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# End-to-end check for the order pipeline (step 11).
#
# Proves that a POST to the public API flows through the WHOLE stack:
#   nginx -> Flask -> MySQL (orders)
#                    -> Kafka (order-events) -> payment-group + delivery-group
#                    -> RabbitMQ (order-notifications) -> rabbitmq-worker
#
# Prerequisite: the compose stack is running and reachable.
# Usage:    bash scripts/e2e_test.sh [API_BASE_URL]
#           default API_BASE_URL = http://localhost:8080   (nginx)
# ---------------------------------------------------------------------------
set -uo pipefail

API_BASE="${1:-http://localhost:8080}"
ENDPOINT="$API_BASE/api/orders"

buyers=(Alice Bob Carol)
products=(Widget Gadget Gizmo)
amounts=(19.99 42.00 7.50)

pass=0; fail=0
ok() { echo "  [PASS] $1"; pass=$((pass+1)); }
ko() { echo "  [FAIL] $1"; fail=$((fail+1)); }

mysql_count() {
  docker compose exec -T mysql mysql -uapp -papppass orders_db -N -s \
    -e "SELECT COUNT(*) FROM orders" 2>/dev/null
}

echo "== 1. Baseline =="
baseline="$(mysql_count)"; baseline="${baseline:-0}"
echo "   orders in MySQL before: $baseline"

echo "== 2. POST ${#buyers[@]} orders -> $ENDPOINT =="
for i in "${!buyers[@]}"; do
  code="$(curl -s -o /tmp/e2e_resp.json -w "%{http_code}" -X POST "$ENDPOINT" \
    -H 'Content-Type: application/json' \
    -d "{\"buyer\":\"${buyers[$i]}\",\"product\":\"${products[$i]}\",\"amount\":${amounts[$i]}}")"
  echo "   ${buyers[$i]}: HTTP $code -> $(cat /tmp/e2e_resp.json)"
done

echo "== 3. MySQL has the new rows =="
cur="$baseline"
for _ in $(seq 1 15); do
  cur="$(mysql_count)"; cur="${cur:-0}"
  [ "$cur" -ge $((baseline + ${#buyers[@]})) ] && break
  sleep 1
done
if [ "${cur:-0}" -ge $((baseline + ${#buyers[@]})) ]; then
  ok "MySQL now has $cur orders (was $baseline)"
else
  ko "MySQL has $cur orders, expected >= $((baseline + ${#buyers[@]}))"
fi

echo "== 4. Kafka: both consumer groups present =="
groups="$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null || true)"
grep -qx 'payment-group'  <<<"$groups" && ok "kafka group payment-group present"  || ko "kafka group payment-group missing"
grep -qx 'delivery-group' <<<"$groups" && ok "kafka group delivery-group present" || ko "kafka group delivery-group missing"

echo "== 5. Kafka partitions: order-events per-partition offsets (informational) =="
for g in payment-group delivery-group; do
  echo "   --- $g ---"
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$g" 2>/dev/null \
    | grep -E '^GROUP|order-events' | head -5 || true
done

echo "== 6. Kafka consumers processed the orders (logs) =="
docker compose logs kafka-consumer-payment 2>&1 | grep -q "payment processed for order #" \
  && ok "payment consumer logged processed orders" \
  || ko "payment consumer has no 'payment processed' lines"
docker compose logs kafka-consumer-delivery 2>&1 | grep -q "доставка по order_id=" \
  && ok "delivery consumer logged prepared deliveries" \
  || ko "delivery consumer has no 'доставка по order_id' lines"

echo "== 7. RabbitMQ worker delivered the notifications =="
wlog="$(docker compose logs rabbitmq-worker 2>&1 || true)"
hits="$(grep -c 'notification delivered to customer' <<<"$wlog" || true)"
echo "   total notifications delivered: ${hits:-0}"
for b in "${buyers[@]}"; do
  grep -q "accepted: $b " <<<"$wlog" \
    && ok "worker delivered notification for '$b'" \
    || ko "no worker notification for '$b'"
done

echo
echo "RESULT: $pass passed, $fail failed"
if [ "$fail" -eq 0 ]; then
  echo "ALL GREEN ✅"
  exit 0
else
  echo "SOME CHECKS FAILED ❌"
  exit 1
fi

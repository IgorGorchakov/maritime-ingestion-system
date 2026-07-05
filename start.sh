# chmod +x /Users/mac/Desktop/drafts/maritime-ingestion-system/start.sh
# ./start.sh
set -euo pipefail

echo "=== 1/3  Building all modules ==="
./mvnw clean install -DskipTests -Dmaven.test.skip=true -q

echo "=== 2/3  Starting infrastructure (Kafka, Schema Registry, Postgres, Prometheus, Grafana, Frontend) ==="
docker compose up -d

echo "Waiting for Kafka to be ready..."
until docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; do
  sleep 2
done
echo "Kafka is ready."

echo "Waiting for Schema Registry to be ready..."
until curl -sf http://localhost:8085/subjects >/dev/null 2>&1; do
  sleep 2
done
echo "Schema Registry is ready."

echo "=== 3/3  Starting Spring Boot services ==="
./mvnw -pl maritime-enricher  spring-boot:run -Dmaven.test.skip=true > /tmp/enricher.log  2>&1 &
echo "  enricher   (pid $!) → :8082  log: /tmp/enricher.log"
sleep 5

./mvnw -pl maritime-detection spring-boot:run -Dmaven.test.skip=true > /tmp/detection.log 2>&1 &
echo "  detection  (pid $!) → :8086  log: /tmp/detection.log"

./mvnw -pl maritime-storage   spring-boot:run -Dmaven.test.skip=true > /tmp/storage.log   2>&1 &
echo "  storage    (pid $!) → :8083  log: /tmp/storage.log"

./mvnw -pl maritime-api       spring-boot:run -Dmaven.test.skip=true > /tmp/api.log       2>&1 &
echo "  api        (pid $!) → :8084  log: /tmp/api.log"

./mvnw -pl maritime-ingestion spring-boot:run -Dmaven.test.skip=true > /tmp/ingestion.log 2>&1 &
echo "  ingestion  (pid $!) → :8081  log: /tmp/ingestion.log"

echo ""
echo "=== All services starting ==="
echo ""
echo "  Frontend:        http://localhost:5173"
echo "  Grafana:         http://localhost:3000  (admin/admin)"
echo "  Prometheus:      http://localhost:9090"
echo ""
echo "  Start simulation:  curl -X POST http://localhost:8081/api/v1/simulate/start"
echo "  Stop simulation:   curl -X POST http://localhost:8081/api/v1/simulate/stop"
echo ""
echo "  Tail logs:         tail -f /tmp/{enricher,detection,storage,api,ingestion}.log"
echo "  Stop services:     make stop-all"
echo "  Stop infra:        docker compose down"

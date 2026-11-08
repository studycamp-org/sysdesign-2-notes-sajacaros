#!/usr/bin/env bash
# jar 를 만들고 도커로 띄운다. 끝낼 때: docker compose down (데이터까지 지우려면 down -v)
set -euo pipefail
cd "$(dirname "$0")"
./gradlew -q bootJar
docker compose up --build -d "$@"
echo
echo "대시보드: http://localhost:8081"
echo "로그 보기: docker compose logs -f producer consumer-search consumer-anomaly"

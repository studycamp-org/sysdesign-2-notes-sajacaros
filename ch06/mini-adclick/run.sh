#!/usr/bin/env bash
# 사용법: ./run.sh            (빌드하고 띄운다)
# 끝낼 때: docker compose down -v   (Kafka·ClickHouse·RustFS 는 메모리에만 있어 멈추면 사라진다)
set -euo pipefail
cd "$(dirname "$0")"
docker compose up --build -d --remove-orphans
echo
echo "대시보드   : http://localhost:8006"
echo "Flink UI   : http://localhost:8081"
echo "ClickHouse : http://localhost:8123/play  (user default / password demo)"
echo "RustFS 콘솔: http://localhost:9001       (rustfsadmin / rustfsadmin)"
echo "로그       : docker compose logs -f api recon"

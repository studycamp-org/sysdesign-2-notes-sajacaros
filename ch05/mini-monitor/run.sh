#!/usr/bin/env bash
# 사용법: ./run.sh [push 시뮬레이터 수] [pull 시뮬레이터 수]   (기본 4 4)
# 실행 중에 다시 부르면 시뮬레이터 수만 바뀐다. 끝낼 때: docker compose down (데이터는 메모리에만 있어 멈추면 사라진다)
set -euo pipefail
cd "$(dirname "$0")"
PUSH=${1:-4}
PULL=${2:-4}
docker compose up --build -d --scale sim-push="$PUSH" --scale sim-pull="$PULL"
echo
echo "파이프라인 뷰: http://localhost:8000"
echo "Grafana     : http://localhost:3000/d/mini-monitor"
echo "로그        : docker compose logs -f alerter notifier"

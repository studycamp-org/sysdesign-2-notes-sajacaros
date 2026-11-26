#!/usr/bin/env bash
# 사용법: ./run.sh            (빌드하고 띄운다)
# 끝낼 때: docker compose down  (DB·캐시는 메모리에만 있어 멈추면 사라진다)
set -euo pipefail
cd "$(dirname "$0")"
docker compose up --build -d --remove-orphans
echo
echo "호텔 예약 사이트 : http://localhost:8007   (오른쪽 아래 </> 버튼 = 개발자 패널)"
echo "호텔 직원 관리자 : http://localhost:8008"
echo "PostgreSQL       : psql postgresql://postgres:postgres@localhost:15437/hotel"
echo "로그             : docker compose logs -f reservation payment"

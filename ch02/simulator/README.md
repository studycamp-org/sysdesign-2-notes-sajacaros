# 근접 친구(Nearby Friends) 시뮬레이터

「가상 면접 사례로 배우는 대규모 시스템 설계 기초 2」 2장 시연용.

## 실행

```bash
docker compose up -d                                   # Redis
uv run uvicorn app.main:app --port 8000               # 백엔드
# 브라우저: http://localhost:8000
```

## 구조

| 책의 구성요소 | 이 구현 |
|---|---|
| 위치 캐시 | Redis `location:{uid}` (TTL, `LOCATION_TTL` 기본 3600초) |
| Redis Pub/Sub | 사용자마다 채널 `friend_loc:{uid}` |
| WebSocket 서버 | 위치 송신은 HTTP POST, 수신은 SSE 핸들러 |
| 친구 관계 | 모두 서로 친구로 가정 (`friends_of`) |

### 초기화 (사용자 선택 시)
1. `POST /api/users/{uid}/location`: 내 위치를 캐시에 SET하고 내 채널에 PUBLISH
2. `GET /api/users/{uid}/stream?radius=500` (SSE). 핸들러가 다음 순서로 처리한다.
   - 캐시에서 내 위치 GET
   - 친구 채널과 내 채널 SUBSCRIBE (MGET 전에 구독해서 그 사이 갱신을 놓치지 않음)
   - 친구 전체 위치 MGET → **핸들러 메모리에 저장**
   - 반경 필터 → `init` 이벤트로 근처 친구 전송

### 이후
- 내 이동: HTTP POST → PUBLISH → 내 핸들러가 수신 → 메모리의 친구 위치로 재계산 → `nearby_list`
- 친구 이동: 친구 채널 메시지 → 메모리 갱신 → 반경 안이면 `friend_update`, 반경을 벗어나는 순간 `friend_out`, 그 외에는 버림(필터링)

`/api/admin/stream`은 맵에 모든 사용자를 그리기 위한 관제용 스트림이다. 노란색 표시는 이 스트림이 아니라 선택한 사용자의 SSE만 보고 정한다.

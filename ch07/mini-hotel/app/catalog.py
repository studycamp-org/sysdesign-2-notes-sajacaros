"""처음 띄울 때 넣는 호텔 목록.

실제로는 호텔 직원이 관리자 패널로 호텔·객실을 등록하면 Hotel Management Service 가
Hotel·Rate·Reservation Service 에 각각 알려 각자 자기 스키마에 행을 만든다.
여기서는 서비스마다 이 목록을 읽어 자기 몫만 넣는다(다른 서비스 스키마는 건드리지 않는다).
"""

HOTELS = [
    {
        "hotel_id": 1, "name": "한강 리버뷰 호텔", "city": "서울", "stars": 5, "scene": "river",
        "address": "서울 영등포구 여의동로 330",
        "description": "여의도 한강공원 바로 앞. 모든 객실에서 강이 보이고, 루프톱 라운지에서 밤섬 야경을 즐길 수 있습니다.",
        "types": [
            {"room_type_id": 101, "name": "스탠다드 더블", "beds": "더블 침대 1개", "size": 26, "cap": 2, "rooms": 20, "base": 159000,
             "amen": ["무료 Wi-Fi", "에어컨", "미니바", "레인 샤워"]},
            {"room_type_id": 102, "name": "디럭스 리버뷰 킹", "beds": "킹 침대 1개", "size": 34, "cap": 2, "rooms": 10, "base": 229000,
             "amen": ["한강 전망", "욕조", "네스프레소", "무료 Wi-Fi"]},
            {"room_type_id": 103, "name": "코너 스위트", "beds": "킹 침대 1개 + 소파베드", "size": 58, "cap": 3, "rooms": 3, "base": 489000,
             "amen": ["270° 전망", "라운지 이용", "거실 분리", "욕조"]},
        ],
    },
    {
        "hotel_id": 2, "name": "을지로 스테이", "city": "서울", "stars": 3, "scene": "city",
        "address": "서울 중구 을지로 157",
        "description": "을지로 골목 한가운데의 부티크 호텔. 2호선·3호선 을지로3가역에서 걸어서 2분입니다.",
        "types": [
            {"room_type_id": 201, "name": "스탠다드 퀸", "beds": "퀸 침대 1개", "size": 18, "cap": 2, "rooms": 20, "base": 89000,
             "amen": ["무료 Wi-Fi", "에어컨", "스마트 TV"]},
            {"room_type_id": 202, "name": "트윈", "beds": "싱글 침대 2개", "size": 20, "cap": 2, "rooms": 10, "base": 99000,
             "amen": ["무료 Wi-Fi", "에어컨", "책상"]},
            {"room_type_id": 203, "name": "루프톱 테라스 룸", "beds": "퀸 침대 1개", "size": 28, "cap": 2, "rooms": 3, "base": 169000,
             "amen": ["전용 테라스", "블루투스 스피커", "무료 Wi-Fi"]},
        ],
    },
    {
        "hotel_id": 3, "name": "해운대 오션 스위트", "city": "부산", "stars": 4, "scene": "sea",
        "address": "부산 해운대구 해운대해변로 264",
        "description": "해운대 해수욕장까지 걸어서 1분. 오션뷰 객실과 인피니티 풀을 갖춘 호텔입니다.",
        "types": [
            {"room_type_id": 301, "name": "시티뷰 더블", "beds": "더블 침대 1개", "size": 24, "cap": 2, "rooms": 20, "base": 119000,
             "amen": ["무료 Wi-Fi", "에어컨", "미니바"]},
            {"room_type_id": 302, "name": "오션뷰 트윈", "beds": "싱글 침대 2개", "size": 30, "cap": 3, "rooms": 10, "base": 179000,
             "amen": ["바다 전망", "발코니", "무료 Wi-Fi"]},
            {"room_type_id": 303, "name": "오션 패밀리 스위트", "beds": "킹 1개 + 싱글 2개", "size": 62, "cap": 4, "rooms": 3, "base": 399000,
             "amen": ["바다 전망", "주방", "거실 분리", "세탁기"]},
        ],
    },
    {
        "hotel_id": 4, "name": "제주 돌담 리조트", "city": "제주", "stars": 4, "scene": "island",
        "address": "제주 서귀포시 중문관광로 72",
        "description": "현무암 돌담과 귤밭에 둘러싸인 저층 리조트. 객실마다 작은 정원이 딸려 있습니다.",
        "types": [
            {"room_type_id": 401, "name": "가든 더블", "beds": "더블 침대 1개", "size": 28, "cap": 2, "rooms": 20, "base": 139000,
             "amen": ["전용 정원", "무료 Wi-Fi", "조식 포함"]},
            {"room_type_id": 402, "name": "한라산뷰 킹", "beds": "킹 침대 1개", "size": 36, "cap": 2, "rooms": 10, "base": 209000,
             "amen": ["한라산 전망", "욕조", "조식 포함"]},
            {"room_type_id": 403, "name": "독채 풀빌라", "beds": "킹 침대 2개", "size": 85, "cap": 4, "rooms": 3, "base": 590000,
             "amen": ["개별 수영장", "바비큐 그릴", "주방", "조식 포함"]},
        ],
    },
    {
        "hotel_id": 5, "name": "경포 솔숲 호텔", "city": "강릉", "stars": 3, "scene": "forest",
        "address": "강원 강릉시 창해로 307",
        "description": "경포호와 바다 사이 소나무 숲 속 호텔. 자전거를 무료로 빌려 드립니다.",
        "types": [
            {"room_type_id": 501, "name": "스탠다드 트윈", "beds": "싱글 침대 2개", "size": 22, "cap": 2, "rooms": 20, "base": 95000,
             "amen": ["무료 Wi-Fi", "에어컨", "자전거 대여"]},
            {"room_type_id": 502, "name": "포레스트 킹", "beds": "킹 침대 1개", "size": 30, "cap": 2, "rooms": 10, "base": 139000,
             "amen": ["숲 전망", "발코니", "무료 Wi-Fi"]},
            {"room_type_id": 503, "name": "온돌 패밀리", "beds": "온돌 + 퀸 침대", "size": 45, "cap": 5, "rooms": 3, "base": 219000,
             "amen": ["온돌방", "주방", "바다 전망"]},
        ],
    },
]

GUESTS = [("김민수", "minsu@example.com"), ("이서연", "seoyeon@example.com"), ("박지훈", "jihoon@example.com")]

package study.minimq.common;

import java.util.List;

/**
 * 소비자·팔로워가 오프셋을 주고 받아 가는 묶음 (그림 4.14).
 * logStartOffset 앞은 보관 기간이 지나 지워졌다. logEndOffset(LEO) 은 다음에 쓰일 오프셋.
 * highWatermark(HW, 교재의 합의 오프셋) 앞까지가 ISR 모두에 복제된 메시지다. 소비자는 HW 앞까지만 받는다.
 */
public record FetchResponse(String topic, int partition, List<Message> messages,
                            long logStartOffset, long logEndOffset, long highWatermark) {
}

package study.minimq.common;

import java.util.List;

/**
 * 소비자가 오프셋을 주고 받아 가는 묶음 (그림 4.14).
 * logStartOffset 앞은 보관 기간이 지나 지워졌다. logEndOffset 은 다음에 쓰일 오프셋(LEO).
 */
public record FetchResponse(String topic, int partition, List<Message> messages, long logStartOffset, long logEndOffset) {
}

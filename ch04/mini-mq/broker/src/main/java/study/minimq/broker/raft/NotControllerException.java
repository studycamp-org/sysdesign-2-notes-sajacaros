package study.minimq.broker.raft;

/** 컨트롤러(리더)만 할 수 있는 일을 다른 노드가 받았거나, 커밋 전에 리더 자리를 잃었다. */
public class NotControllerException extends RuntimeException {
    private final Integer leaderId;

    public NotControllerException(Integer leaderId) {
        super(leaderId == null ? "컨트롤러가 아직 없다 (선출 중)" : "컨트롤러는 broker-" + leaderId);
        this.leaderId = leaderId;
    }

    public Integer leaderId() {
        return leaderId;
    }
}

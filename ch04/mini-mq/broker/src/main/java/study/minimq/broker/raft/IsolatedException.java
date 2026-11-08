package study.minimq.broker.raft;

/** 고립 실험 중이라 RPC 를 받지 않는다. */
public class IsolatedException extends RuntimeException {
    public IsolatedException() {
        super("이 브로커는 고립 실험 중");
    }
}

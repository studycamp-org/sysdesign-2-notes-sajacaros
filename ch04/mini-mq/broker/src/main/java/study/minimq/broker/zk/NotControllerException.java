package study.minimq.broker.zk;

/** 컨트롤러만 할 수 있는 일을 다른 브로커가 받았거나, 쓰는 사이 컨트롤러 자리를 잃었다. */
public class NotControllerException extends RuntimeException {
    public NotControllerException(Integer controllerId) {
        super(controllerId == null ? "컨트롤러가 아직 없다 (선출 중)" : "컨트롤러는 broker-" + controllerId);
    }
}

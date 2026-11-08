package study.minimq.broker.raft;

/** seq 는 노드 안에서의 순서. 같은 밀리초에 여러 일이 일어나도 순서를 잃지 않게 한다. */
public record RaftEvent(long at, long seq, int node, long term, String text) {
}

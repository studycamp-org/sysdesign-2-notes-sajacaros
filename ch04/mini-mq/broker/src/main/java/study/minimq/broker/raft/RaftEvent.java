package study.minimq.broker.raft;

/**
 * @param seq  노드 안에서의 순서. 같은 밀리초에 여러 일이 일어나도 순서를 잃지 않게 한다
 * @param kind 종류: start | timeout(출마) | vote(투표) | deny(투표 거절) | stepdown(더 높은 term 을 보고 내려옴)
 *             | elected(당선) | follow(새 리더를 따름) | reject(낡은 박동 거부) | commit | isolate | note
 * @param peer 상대 브로커 (투표한 후보, 따르는 리더 등). 없으면 null
 */
public record RaftEvent(long at, long seq, int node, long term, String kind, Integer peer, String text) {
}

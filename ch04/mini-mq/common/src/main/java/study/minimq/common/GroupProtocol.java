package study.minimq.common;

import java.util.List;
import java.util.Map;

/**
 * 소비자 그룹 재조정 프로토콜 (그림 4.15~4.20). 코디네이터 = 그룹 이름 해시로 정한 살아 있는 브로커 (Topics.coordinatorFor).
 *
 * <ol>
 *   <li>join: 모든 멤버가 (다시) 합류한다. 코디네이터는 모두 오거나 시간이 다 되면 세대를 올리고 리더 소비자를 뽑는다</li>
 *   <li>sync: 리더 소비자가 파티션 배치 계획을 보내면, 코디네이터가 각 멤버에게 자기 몫을 돌려준다</li>
 *   <li>heartbeat: 살아 있다고 알린다. 응답에 "다시 합류하라"가 실려 온다 (코디네이터의 알림은 수동적)</li>
 *   <li>leave: 스스로 나간다</li>
 * </ol>
 */
public final class GroupProtocol {
    private GroupProtocol() {
    }

    /** memberId 가 null 이면 처음 합류. */
    public record JoinRequest(String group, String memberId, String clientId, String topic) {
    }

    /** members 는 리더에게만 채워 준다 (리더가 배치 계획을 만든다). */
    public record JoinResponse(String memberId, int generation, String leaderId, List<String> members, String topic) {
    }

    /** assignments 는 리더만 보낸다. */
    public record SyncRequest(String group, int generation, String memberId, Map<String, List<Integer>> assignments) {
    }

    public record SyncResponse(int generation, List<Integer> partitions) {
    }

    public record HeartbeatRequest(String group, int generation, String memberId) {
    }

    /** command: 대시보드 실험용 지시 (crash = 박동을 끊고 멈춤, leave = 나가기). */
    public record HeartbeatResponse(boolean rebalance, String command) {
    }

    public record LeaveRequest(String group, String memberId) {
    }
}

package study.minimq.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import study.minimq.common.Message;
import study.minimq.common.ProduceRecord;

class PartitionLogTest {

    @TempDir
    Path dir;

    private static List<ProduceRecord> records(int from, int n) {
        return IntStream.range(from, from + n).mapToObj(i -> new ProduceRecord("k", "v" + i, i)).toList();
    }

    @Test
    void 오프셋은_단조_증가하고_세그먼트를_넘어_읽힌다() {
        PartitionLog log = new PartitionLog("t", 0, dir, 500, false);
        long[] a = log.appendAsLeader(records(0, 10), 0);
        long[] b = log.appendAsLeader(records(10, 10), 0);
        long[] c = log.appendAsLeader(records(20, 10), 0);

        assertThat(a).containsExactly(0, 9);
        assertThat(b).containsExactly(10, 19);
        assertThat(c).containsExactly(20, 29);
        assertThat(log.state().segments()).hasSizeGreaterThan(1); // 500바이트마다 새 세그먼트

        List<Message> read = log.read(5, 20, Long.MAX_VALUE);
        assertThat(read).extracting(Message::offset).containsExactlyElementsOf(
                IntStream.range(5, 25).mapToObj(i -> (long) i).toList());
        assertThat(read).allMatch(Message::crcValid);
    }

    @Test
    void 재시작하면_파일에서_복구하고_쓰다_만_꼬리는_잘라낸다() throws Exception {
        PartitionLog log = new PartitionLog("t", 0, dir, 1 << 20, false);
        log.appendAsLeader(records(0, 5), 0);
        log.close();

        // 쓰다가 죽은 것처럼 길이만 쓰고 내용이 없는 꼬리를 붙인다
        Path seg = dir.resolve(LogSegment.fileName(0));
        Files.write(seg, new byte[]{0, 0, 0, 100, 1, 2}, StandardOpenOption.APPEND);

        PartitionLog reopened = new PartitionLog("t", 0, dir, 1 << 20, false);
        assertThat(reopened.logEndOffset()).isEqualTo(5);
        assertThat(reopened.appendAsLeader(records(5, 1), 0)).containsExactly(5, 5);
        assertThat(reopened.read(0, 100, Long.MAX_VALUE)).hasSize(6);
    }

    @Test
    void 롱_폴링은_새_메시지가_오면_깨어난다() {
        PartitionLog log = new PartitionLog("t", 0, dir, 1 << 20, false);
        CompletableFuture<Void> leo = log.awaitLogEnd(1);
        CompletableFuture<Void> hw = log.awaitHighWatermark(1);
        log.appendAsLeader(records(0, 1), 0);
        assertThat(leo).isDone();
        assertThat(hw).as("HW 가 오르기 전에는 소비자를 깨우지 않는다").isNotDone();
        log.advanceHighWatermark(1);
        assertThat(hw).isDone();
    }

    @Test
    void 보관_기간이_지난_세그먼트는_앞에서부터_지운다() {
        PartitionLog log = new PartitionLog("t", 0, dir, 200, false);
        for (int i = 0; i < 5; i++) log.appendAsLeader(records(i * 5, 5), 0);
        log.advanceHighWatermark(log.logEndOffset());
        int before = log.state().segments().size();

        int deleted = log.deleteExpired(-1); // 전부 만료로 간주
        assertThat(deleted).isEqualTo(before - 1); // 활성 세그먼트는 남는다
        assertThat(log.logStartOffset()).isGreaterThan(0);
        assertThat(log.read(0, 100, Long.MAX_VALUE)).first().extracting(Message::offset).isEqualTo(log.logStartOffset());
    }

    @Test
    void 리더_epoch_로_갈라진_꼬리를_찾아_잘라낸다() {
        // 옛 리더(epoch 0): 0~9 를 받았는데 6~9 는 팔로워에 복제되기 전에 죽었다
        PartitionLog oldLeader = new PartitionLog("t", 0, dir.resolve("old"), 1 << 20, false);
        oldLeader.assignEpoch(0);
        oldLeader.appendAsLeader(records(0, 10), 0);

        // 새 리더(epoch 1): 0~5 까지만 있었고, 리더가 된 뒤 새 메시지 3건을 받았다
        PartitionLog newLeader = new PartitionLog("t", 0, dir.resolve("new"), 1 << 20, false);
        newLeader.appendReplica(oldLeader.read(0, 6, Long.MAX_VALUE));
        newLeader.assignEpoch(1);
        newLeader.appendAsLeader(records(100, 3), 1);

        // 옛 리더가 돌아와 팔로워가 된다: "내 마지막 epoch 0 은 너에게서 어디까지?"
        long end = newLeader.endOffsetForEpoch(oldLeader.latestEpoch());
        assertThat(end).isEqualTo(6);
        oldLeader.truncateTo(end);
        assertThat(oldLeader.logEndOffset()).isEqualTo(6);

        // 그 뒤로는 새 리더의 것을 그대로 받는다
        oldLeader.appendReplica(newLeader.read(6, 100, Long.MAX_VALUE));
        assertThat(oldLeader.read(0, 100, Long.MAX_VALUE)).extracting(Message::value)
                .containsExactlyElementsOf(newLeader.read(0, 100, Long.MAX_VALUE).stream().map(Message::value).toList());
        assertThat(oldLeader.latestEpoch()).isEqualTo(1);
    }
}

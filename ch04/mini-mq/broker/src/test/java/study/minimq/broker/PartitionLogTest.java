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
        long[] a = log.append(records(0, 10));
        long[] b = log.append(records(10, 10));
        long[] c = log.append(records(20, 10));

        assertThat(a).containsExactly(0, 9);
        assertThat(b).containsExactly(10, 19);
        assertThat(c).containsExactly(20, 29);
        assertThat(log.state().segments()).hasSizeGreaterThan(1); // 500바이트마다 새 세그먼트

        List<Message> read = log.read(5, 20);
        assertThat(read).extracting(Message::offset).containsExactlyElementsOf(
                IntStream.range(5, 25).mapToObj(i -> (long) i).toList());
        assertThat(read).allMatch(Message::crcValid);
    }

    @Test
    void 재시작하면_파일에서_복구하고_쓰다_만_꼬리는_잘라낸다() throws Exception {
        PartitionLog log = new PartitionLog("t", 0, dir, 1 << 20, false);
        log.append(records(0, 5));
        log.close();

        // 쓰다가 죽은 것처럼 길이만 쓰고 내용이 없는 꼬리를 붙인다
        Path seg = dir.resolve(LogSegment.fileName(0));
        Files.write(seg, new byte[]{0, 0, 0, 100, 1, 2}, StandardOpenOption.APPEND);

        PartitionLog reopened = new PartitionLog("t", 0, dir, 1 << 20, false);
        assertThat(reopened.logEndOffset()).isEqualTo(5);
        assertThat(reopened.append(records(5, 1))).containsExactly(5, 5);
        assertThat(reopened.read(0, 100)).hasSize(6);
    }

    @Test
    void 롱_폴링은_새_메시지가_오면_깨어난다() {
        PartitionLog log = new PartitionLog("t", 0, dir, 1 << 20, false);
        CompletableFuture<Void> waiting = log.awaitData(0);
        assertThat(waiting).isNotDone();
        log.append(records(0, 1));
        assertThat(waiting).isDone();
    }

    @Test
    void 보관_기간이_지난_세그먼트는_앞에서부터_지운다() {
        PartitionLog log = new PartitionLog("t", 0, dir, 200, false);
        for (int i = 0; i < 5; i++) log.append(records(i * 5, 5));
        int before = log.state().segments().size();

        int deleted = log.deleteExpired(-1); // 전부 만료로 간주
        assertThat(deleted).isEqualTo(before - 1); // 활성 세그먼트는 남는다
        assertThat(log.logStartOffset()).isGreaterThan(0);
        assertThat(log.read(0, 100)).first().extracting(Message::offset).isEqualTo(log.logStartOffset());
    }
}

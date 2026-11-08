package study.minimq.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;

import study.minimq.common.Json;
import study.minimq.common.Message;
import study.minimq.common.ProduceRecord;

/**
 * 파티션 사본 하나 = 세그먼트 파일의 나열. 활성 세그먼트(마지막)에만 쓰고, 한계에 닿으면 새 세그먼트를 연다.
 *
 * <ul>
 *   <li>LEO (log end offset): 다음에 쓰일 오프셋</li>
 *   <li>HW (high watermark, 교재의 합의 오프셋): 이 앞까지는 ISR 모두에 있다. 소비자는 HW 앞까지만 읽는다</li>
 *   <li>리더 epoch 캐시: epoch 마다 그 epoch 의 첫 오프셋. 리더가 바뀐 뒤 팔로워가 어디서부터
 *       갈라졌는지 찾아 잘라내는 데 쓴다 (교재 밖, 카프카 KIP-101). 파일 leader-epoch-checkpoint</li>
 * </ul>
 */
final class PartitionLog {
    private static final Pattern SEGMENT = Pattern.compile("segment-(\\d+)\\.log");

    private record Waiter(long target, CompletableFuture<Void> future) {
    }

    private final String topic;
    private final int partition;
    private final Path dir;
    private final Path epochFile;
    private final long segmentBytes;
    private final boolean flushEveryWrite;
    private final List<LogSegment> segments = new ArrayList<>();
    private final TreeMap<Integer, Long> epochs = new TreeMap<>(); // epoch → 그 epoch 의 첫 오프셋
    private final List<Waiter> leoWaiters = new ArrayList<>();
    private final List<Waiter> hwWaiters = new ArrayList<>();
    private long highWatermark;

    PartitionLog(String topic, int partition, Path dir, long segmentBytes, boolean flushEveryWrite) {
        this.topic = topic;
        this.partition = partition;
        this.dir = dir;
        this.epochFile = dir.resolve("leader-epoch-checkpoint");
        this.segmentBytes = segmentBytes;
        this.flushEveryWrite = flushEveryWrite;
        try {
            Files.createDirectories(dir);
            try (Stream<Path> files = Files.list(dir)) {
                List<Long> bases = files.map(p -> SEGMENT.matcher(p.getFileName().toString()))
                        .filter(Matcher::matches)
                        .map(m -> Long.parseLong(m.group(1)))
                        .sorted()
                        .toList();
                for (long base : bases) segments.add(LogSegment.open(dir.resolve(LogSegment.fileName(base)), base));
            }
            if (segments.isEmpty()) segments.add(LogSegment.open(dir.resolve(LogSegment.fileName(0)), 0));
            if (Files.exists(epochFile)) {
                Map<Integer, Long> m = Json.MAPPER.readValue(epochFile.toFile(), new TypeReference<Map<Integer, Long>>() {});
                epochs.putAll(m);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        epochs.values().removeIf(start -> start > logEndOffset());
    }

    // ------------------------------------------------------------------ 쓰기

    /** 리더로서 묶음을 끝에 붙이고 [첫 오프셋, 마지막 오프셋] 을 돌려준다. */
    synchronized long[] appendAsLeader(List<ProduceRecord> records, int leaderEpoch) {
        long base = logEndOffset();
        if (records.isEmpty()) return new long[]{base, base - 1};
        List<Message> msgs = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) msgs.add(Message.create(topic, partition, base + i, leaderEpoch, records.get(i)));
        write(msgs);
        return new long[]{base, base + records.size() - 1};
    }

    /** 팔로워로서 리더에게서 가져온 메시지를 오프셋 그대로 붙인다. */
    synchronized void appendReplica(List<Message> msgs) {
        if (msgs.isEmpty()) return;
        long expected = logEndOffset();
        if (msgs.get(0).offset() != expected) {
            throw new IllegalStateException(topic + "-" + partition + " 복제 오프셋 불일치: LEO " + expected + ", 받은 것 " + msgs.get(0).offset());
        }
        write(msgs);
    }

    private void write(List<Message> msgs) {
        try {
            if (active().bytes() >= segmentBytes) roll();
            List<byte[]> payloads = new ArrayList<>(msgs.size());
            for (Message m : msgs) {
                payloads.add(Json.MAPPER.writeValueAsBytes(m));
                Map.Entry<Integer, Long> last = epochs.lastEntry();
                if (last == null || m.leaderEpoch() > last.getKey()) epochs.put(m.leaderEpoch(), m.offset());
            }
            active().appendAll(payloads);
            if (flushEveryWrite) active().flush();
            saveEpochs();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        wake(leoWaiters, logEndOffset());
    }

    /** 리더가 되면 이번 epoch 의 시작 오프셋을 기록한다. */
    synchronized void assignEpoch(int epoch) {
        Map.Entry<Integer, Long> last = epochs.lastEntry();
        if (last == null || epoch > last.getKey()) {
            epochs.put(epoch, logEndOffset());
            saveEpochs();
        }
    }

    /**
     * 리더가 답한다: "너의 epoch 는 내 로그에서 어디까지인가". 그 epoch 다음 epoch 의 시작 오프셋,
     * 다음 epoch 가 없으면 내 LEO. 팔로워는 자기 LEO 와 이 값 중 작은 곳까지 남기고 잘라낸다.
     */
    synchronized long endOffsetForEpoch(int epoch) {
        Map.Entry<Integer, Long> next = epochs.higherEntry(epoch);
        return next == null ? logEndOffset() : next.getValue();
    }

    synchronized int latestEpoch() {
        return epochs.isEmpty() ? -1 : epochs.lastKey();
    }

    /** offset 부터 뒤를 잘라낸다. 리더가 바뀌어 내 꼬리가 새 리더의 로그와 갈라졌을 때. */
    synchronized void truncateTo(long offset) {
        if (offset >= logEndOffset()) return;
        try {
            while (segments.size() > 1 && active().baseOffset() >= offset) segments.remove(segments.size() - 1).delete();
            active().truncateTo(offset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        epochs.values().removeIf(start -> start >= offset);
        saveEpochs();
        if (highWatermark > offset) highWatermark = offset;
    }

    private void saveEpochs() {
        try {
            Path tmp = dir.resolve("leader-epoch-checkpoint.tmp");
            Json.MAPPER.writeValue(tmp.toFile(), epochs);
            Files.move(tmp, epochFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------ HW

    /** HW 는 뒤로 가지 않는다(잘라낼 때 말고). 올라가면 ACK=all 로 기다리는 생산자와 롱 폴링 소비자를 깨운다. */
    synchronized void advanceHighWatermark(long hw) {
        long capped = Math.min(hw, logEndOffset());
        if (capped <= highWatermark) return;
        highWatermark = capped;
        wake(hwWaiters, highWatermark);
    }

    synchronized long highWatermark() {
        return highWatermark;
    }

    // ------------------------------------------------------------------ 읽기

    /** from 부터 최대 max 건, upTo 앞까지만. */
    synchronized List<Message> read(long from, int max, long upTo) {
        List<Message> out = new ArrayList<>();
        long offset = Math.max(from, logStartOffset());
        long end = Math.min(upTo, logEndOffset());
        try {
            for (LogSegment s : segments) {
                while (offset >= s.baseOffset() && offset < s.nextOffset() && offset < end && out.size() < max) {
                    out.add(Json.MAPPER.readValue(s.read(offset), Message.class));
                    offset++;
                }
                if (out.size() >= max || offset >= end) break;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    /** LEO 가 target 이상이 되면 완료 (팔로워의 롱 폴링). */
    synchronized CompletableFuture<Void> awaitLogEnd(long target) {
        return await(leoWaiters, target, logEndOffset());
    }

    /** HW 가 target 이상이 되면 완료 (소비자의 롱 폴링, ACK=all 생산자). */
    synchronized CompletableFuture<Void> awaitHighWatermark(long target) {
        return await(hwWaiters, target, highWatermark);
    }

    private static CompletableFuture<Void> await(List<Waiter> waiters, long target, long now) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (now >= target) {
            f.complete(null);
        } else {
            waiters.removeIf(w -> w.future().isDone());
            waiters.add(new Waiter(target, f));
        }
        return f;
    }

    private static void wake(List<Waiter> waiters, long now) {
        for (Iterator<Waiter> it = waiters.iterator(); it.hasNext(); ) {
            Waiter w = it.next();
            if (w.target() <= now || w.future().isDone()) {
                w.future().complete(null);
                it.remove();
            }
        }
    }

    // ------------------------------------------------------------------ 보관

    /** 보관 기간이 지난 세그먼트를 앞에서부터 지운다. 활성 세그먼트와 HW 를 넘는 세그먼트는 남긴다. */
    synchronized int deleteExpired(long retentionMs) {
        long deadline = System.currentTimeMillis() - retentionMs;
        int deleted = 0;
        try {
            while (segments.size() > 1 && segments.get(0).lastModifiedMillis() < deadline
                    && segments.get(1).baseOffset() <= highWatermark) {
                segments.remove(0).delete();
                deleted++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return deleted;
    }

    private void roll() throws IOException {
        active().flush();
        long base = logEndOffset();
        segments.add(LogSegment.open(dir.resolve(LogSegment.fileName(base)), base));
    }

    private LogSegment active() {
        return segments.get(segments.size() - 1);
    }

    synchronized long logStartOffset() {
        return segments.get(0).baseOffset();
    }

    synchronized long logEndOffset() {
        return active().nextOffset();
    }

    synchronized PartitionState state() {
        List<PartitionState.SegmentInfo> infos = new ArrayList<>();
        for (int i = 0; i < segments.size(); i++) {
            LogSegment s = segments.get(i);
            infos.add(new PartitionState.SegmentInfo(s.name(), s.baseOffset(), s.count(), s.bytes(), i == segments.size() - 1));
        }
        return new PartitionState(topic, partition, logStartOffset(), logEndOffset(), highWatermark, infos);
    }

    synchronized void close() {
        for (LogSegment s : segments) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
    }
}

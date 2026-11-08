package study.minimq.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import study.minimq.common.Json;
import study.minimq.common.Message;
import study.minimq.common.ProduceRecord;

/**
 * 파티션 하나 = 세그먼트 파일의 나열. 활성 세그먼트(마지막)에만 쓰고, 한계에 닿으면 새 세그먼트를 연다.
 * 이전 세그먼트는 읽기만 하고, 보관 기간이 지나면 통째로 지운다.
 */
final class PartitionLog {
    private static final Pattern SEGMENT = Pattern.compile("segment-(\\d+)\\.log");

    private final String topic;
    private final int partition;
    private final Path dir;
    private final long segmentBytes;
    private final boolean flushEveryWrite;
    private final List<LogSegment> segments = new ArrayList<>();
    private final List<CompletableFuture<Void>> waiters = new ArrayList<>();

    PartitionLog(String topic, int partition, Path dir, long segmentBytes, boolean flushEveryWrite) {
        this.topic = topic;
        this.partition = partition;
        this.dir = dir;
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 묶음을 끝에 붙이고 [첫 오프셋, 마지막 오프셋] 을 돌려준다. */
    synchronized long[] append(List<ProduceRecord> records) {
        long base = logEndOffset();
        if (records.isEmpty()) return new long[]{base, base - 1};
        try {
            if (active().bytes() >= segmentBytes) roll();
            List<byte[]> payloads = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                payloads.add(Json.MAPPER.writeValueAsBytes(Message.create(topic, partition, base + i, records.get(i))));
            }
            active().appendAll(payloads);
            if (flushEveryWrite) active().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        waiters.forEach(w -> w.complete(null)); // 롱 폴링 중인 소비자 깨우기
        waiters.clear();
        return new long[]{base, base + records.size() - 1};
    }

    synchronized List<Message> read(long from, int max) {
        List<Message> out = new ArrayList<>();
        long offset = Math.max(from, logStartOffset());
        try {
            for (LogSegment s : segments) {
                while (offset >= s.baseOffset() && offset < s.nextOffset() && out.size() < max) {
                    out.add(Json.MAPPER.readValue(s.read(offset), Message.class));
                    offset++;
                }
                if (out.size() >= max) break;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    /** offset 위치에 메시지가 생기면 완료되는 future. 롱 폴링에 쓴다. */
    synchronized CompletableFuture<Void> awaitData(long offset) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (logEndOffset() > offset) {
            f.complete(null);
        } else {
            waiters.removeIf(CompletableFuture::isDone);
            waiters.add(f);
        }
        return f;
    }

    /** 보관 기간이 지난 세그먼트를 앞에서부터 지운다. 활성 세그먼트는 남긴다. */
    synchronized int deleteExpired(long retentionMs) {
        long deadline = System.currentTimeMillis() - retentionMs;
        int deleted = 0;
        try {
            while (segments.size() > 1 && segments.get(0).lastModifiedMillis() < deadline) {
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
        return new PartitionState(topic, partition, logStartOffset(), logEndOffset(), infos);
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

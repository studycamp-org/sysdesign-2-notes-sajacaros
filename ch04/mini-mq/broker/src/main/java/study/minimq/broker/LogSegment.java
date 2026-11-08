package study.minimq.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

/**
 * 세그먼트 파일 하나 (그림 4.9, 4.10). 끝에 추가만 하는 쓰기 우선 로그(WAL).
 *
 * <p>파일 형식: [길이 4바이트][메시지 JSON] 이 반복된다. 오프셋 → 파일 위치는 메모리에 들고 있고,
 * 브로커가 재시작하면 파일을 처음부터 훑어 다시 만든다. 쓰다 만 꼬리는 잘라낸다.
 */
final class LogSegment {
    private final long baseOffset;
    private final Path path;
    private final FileChannel channel;
    private long[] positions = new long[1024];
    private int count;
    private long bytes;

    private LogSegment(Path path, long baseOffset, FileChannel channel) {
        this.path = path;
        this.baseOffset = baseOffset;
        this.channel = channel;
    }

    static LogSegment open(Path path, long baseOffset) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        LogSegment segment = new LogSegment(path, baseOffset, ch);
        segment.recover();
        return segment;
    }

    static String fileName(long baseOffset) {
        return String.format("segment-%020d.log", baseOffset);
    }

    private void recover() throws IOException {
        long size = channel.size();
        long pos = 0;
        ByteBuffer len = ByteBuffer.allocate(4);
        while (pos + 4 <= size) {
            len.clear();
            channel.read(len, pos);
            len.flip();
            int n = len.getInt();
            if (n <= 0 || pos + 4 + n > size) break;
            addPosition(pos);
            pos += 4 + n;
        }
        if (pos < size) channel.truncate(pos); // 쓰다 만 꼬리
        bytes = pos;
    }

    /** 묶음 전체를 버퍼 하나로 만들어 한 번에 순차 쓰기 한다 (교재: 일괄 처리). */
    void appendAll(List<byte[]> payloads) throws IOException {
        int total = 0;
        for (byte[] p : payloads) total += 4 + p.length;
        ByteBuffer buf = ByteBuffer.allocate(total);
        long pos = bytes;
        for (byte[] p : payloads) {
            addPosition(pos);
            buf.putInt(p.length).put(p);
            pos += 4 + p.length;
        }
        buf.flip();
        long at = bytes;
        while (buf.hasRemaining()) at += channel.write(buf, at);
        bytes = at;
    }

    byte[] read(long offset) throws IOException {
        long pos = positions[(int) (offset - baseOffset)];
        ByteBuffer len = ByteBuffer.allocate(4);
        channel.read(len, pos);
        len.flip();
        ByteBuffer data = ByteBuffer.allocate(len.getInt());
        long at = pos + 4;
        while (data.hasRemaining()) {
            int r = channel.read(data, at);
            if (r < 0) break;
            at += r;
        }
        return data.array();
    }

    private void addPosition(long pos) {
        if (count == positions.length) positions = Arrays.copyOf(positions, count * 2);
        positions[count++] = pos;
    }

    void flush() throws IOException {
        channel.force(false);
    }

    void delete() throws IOException {
        channel.close();
        Files.deleteIfExists(path);
    }

    void close() throws IOException {
        channel.close();
    }

    long lastModifiedMillis() throws IOException {
        return Files.getLastModifiedTime(path).toMillis();
    }

    long baseOffset() { return baseOffset; }
    long nextOffset() { return baseOffset + count; }
    int count() { return count; }
    long bytes() { return bytes; }
    String name() { return path.getFileName().toString(); }
}

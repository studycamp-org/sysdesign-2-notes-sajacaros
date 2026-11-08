package study.minimq.common;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public final class Crc {
    private Crc() {
    }

    public static int of(String key, String value) {
        CRC32 crc = new CRC32();
        if (key != null) crc.update(key.getBytes(StandardCharsets.UTF_8));
        crc.update(0);
        if (value != null) crc.update(value.getBytes(StandardCharsets.UTF_8));
        return (int) crc.getValue();
    }
}

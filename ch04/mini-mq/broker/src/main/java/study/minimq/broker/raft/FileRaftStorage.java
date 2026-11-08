package study.minimq.broker.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import study.minimq.common.Json;

/** {dataDir}/raft.json 에 원자적으로 쓴다 (임시 파일 + rename). */
public final class FileRaftStorage implements RaftStorage {
    private final Path file;

    public FileRaftStorage(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = dataDir.resolve("raft.json");
    }

    @Override
    public State load() {
        try {
            return Files.exists(file) ? Json.MAPPER.readValue(file.toFile(), State.class) : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void save(State state) {
        try {
            Path tmp = file.resolveSibling("raft.json.tmp");
            Json.MAPPER.writeValue(tmp.toFile(), state);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

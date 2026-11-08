package study.minimq.zk;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** ZooKeeper-lite 의 HTTP 창구. 데이터는 문자열(JSON)로 주고받는다. */
@RestController
public class ZkApi {
    private final ZkStore store;

    public ZkApi(@Value("${zk.data-dir}") String dataDir, @Value("${zk.session-timeout-ms}") long sessionTimeoutMs) {
        this.store = new ZkStore(Path.of(dataDir), sessionTimeoutMs, System::currentTimeMillis);
    }

    @PostMapping("/session")
    public ZkStore.Session open(@RequestParam String name) {
        return store.openSession(name);
    }

    @PostMapping("/session/ping")
    public void ping(@RequestParam long id) {
        store.ping(id);
    }

    @DeleteMapping("/session")
    public void close(@RequestParam long id) {
        store.closeSession(id);
    }

    @PutMapping("/node")
    public ZkStore.Node create(@RequestParam String path, @RequestParam(required = false) Long session,
                               @RequestParam(defaultValue = "?") String who, @RequestBody(required = false) String data) {
        return store.create(path, data, session, who);
    }

    @GetMapping("/node")
    public ZkStore.Node get(@RequestParam String path) {
        return store.get(path);
    }

    @PostMapping("/node")
    public ZkStore.Node set(@RequestParam String path, @RequestParam(defaultValue = "-1") long expectedVersion,
                            @RequestParam(required = false) String checkPath, @RequestParam(required = false) Long checkCzxid,
                            @RequestParam(defaultValue = "?") String who, @RequestBody(required = false) String data) {
        return store.set(path, data, expectedVersion, checkPath, checkCzxid, who);
    }

    @DeleteMapping("/node")
    public void delete(@RequestParam String path, @RequestParam(defaultValue = "?") String who) {
        store.delete(path, who);
    }

    @GetMapping("/children")
    public List<String> children(@RequestParam String path) {
        return store.children(path);
    }

    /** 감시: zxid 가 since 보다 커질 때까지 최대 waitMs 기다린다. 응답 = 지금 zxid. */
    @GetMapping("/wait")
    public CompletableFuture<Long> waitForChange(@RequestParam long since, @RequestParam(defaultValue = "1000") long waitMs) {
        return store.waitForChange(since).completeOnTimeout(store.zxid(), waitMs, TimeUnit.MILLISECONDS);
    }

    @GetMapping("/admin/state")
    public Map<String, Object> state() {
        return store.state();
    }

    @Scheduled(fixedDelay = 200)
    void expire() {
        store.expireSessions();
    }

    @ExceptionHandler(ZkStore.ZkException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> zkError(ZkStore.ZkException e) {
        return Map.of("code", e.code, "message", e.getMessage());
    }
}

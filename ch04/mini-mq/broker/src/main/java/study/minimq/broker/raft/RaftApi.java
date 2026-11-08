package study.minimq.broker.raft;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RaftApi {
    private final RaftNode raft;

    public RaftApi(RaftNode raft) {
        this.raft = raft;
    }

    @PostMapping("/raft/vote")
    public VoteResponse vote(@RequestBody VoteRequest req) {
        return raft.handleVote(req);
    }

    @PostMapping("/raft/append")
    public AppendResponse append(@RequestBody AppendRequest req) {
        return raft.handleAppend(req);
    }

    @ExceptionHandler(IsolatedException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public String isolated(IsolatedException e) {
        return e.getMessage();
    }
}

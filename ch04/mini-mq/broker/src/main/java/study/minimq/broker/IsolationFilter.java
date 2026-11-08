package study.minimq.broker;

import java.io.IOException;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import study.minimq.broker.zk.Coordination;

/**
 * 고립 실험: 네트워크가 끊긴 것처럼 생산·소비·브로커끼리 요청을 모두 503 으로 막는다 (조정 서비스로 가는 박동도 멈춘다).
 * 대시보드(/, /admin/**)는 관찰과 복구를 위해 열어 둔다.
 */
@Component
public class IsolationFilter extends OncePerRequestFilter {
    private final Coordination coord;

    public IsolationFilter(Coordination coord) {
        this.coord = coord;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String path = req.getRequestURI();
        boolean observable = path.equals("/") || path.equals("/index.html") || path.startsWith("/admin");
        if (coord.isolated() && !observable) {
            res.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "isolated");
            return;
        }
        chain.doFilter(req, res);
    }
}

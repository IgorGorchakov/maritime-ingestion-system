package com.maritime.common.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Establishes a correlation id for every inbound HTTP request at the REST edges
 * (ingestion, api, storage query API).
 *
 * <p>If the caller already sent an {@code X-Correlation-Id} header (e.g. the api service
 * forwarding to storage), it is honoured so the trace stays continuous; otherwise a
 * fresh id is minted. The id is bound to MDC for the duration of the request, echoed
 * back on the response header for the caller, and always cleared in {@code finally}
 * so the servlet thread carries nothing into the next request.
 */
public class CorrelationIdHttpFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String id = request.getHeader(CorrelationIds.HTTP_HEADER);
        if (id == null || id.isBlank()) {
            id = CorrelationIds.newId();
        }
        MDC.put(CorrelationIds.MDC_KEY, id);
        response.setHeader(CorrelationIds.HTTP_HEADER, id);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(CorrelationIds.MDC_KEY);
        }
    }
}

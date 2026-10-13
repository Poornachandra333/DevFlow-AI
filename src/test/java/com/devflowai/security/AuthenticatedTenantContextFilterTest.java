package com.devflowai.security;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthenticatedTenantContextFilterTest {
    private final AuthenticatedTenantContextFilter filter = new AuthenticatedTenantContextFilter();

    @AfterEach
    void clearSecurityContext() {
        TenantContext.clear();
        SecurityContextHolder.clearContext();
    }

    @Test
    void resolvesTenantFromAuthenticatedPrincipalInsteadOfClientHeader() throws IOException, ServletException {
        SecurityContextHolder.getContext().setAuthentication(
                UsernamePasswordAuthenticationToken.authenticated(
                        new TestTenantPrincipal("trusted-tenant"), null, java.util.List.of()));
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("X-Tenant-Id", "untrusted-tenant");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (servletRequest, servletResponse) ->
                assertEquals("trusted-tenant", TenantContext.getTenantId()));

        assertEquals(200, response.getStatus());
        assertThrows(IllegalStateException.class, TenantContext::getTenantId);
    }

    @Test
    void rejectsAuthenticatedPrincipalWithoutTenantIdentity() throws IOException, ServletException {
        SecurityContextHolder.getContext().setAuthentication(
                UsernamePasswordAuthenticationToken.authenticated("user", null, java.util.List.of()));
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(new MockHttpServletRequest(), response, (servletRequest, servletResponse) ->
                assertTrue(false, "request must not reach the chain"));

        assertEquals(403, response.getStatus());
    }

    private record TestTenantPrincipal(String tenantId) implements AuthenticatedTenantPrincipal {
        @Override
        public String getTenantId() {
            return tenantId;
        }
    }
}

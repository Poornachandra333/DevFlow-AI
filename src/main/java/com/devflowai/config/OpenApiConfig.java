package com.devflowai.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI devFlowOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("DevFlow AI Job API")
                        .description("Job orchestration API. In the development profile, tenant identity is supplied "
                                + "by X-Tenant-Id. Other profiles require an authenticated principal implementing "
                                + "AuthenticatedTenantPrincipal; no production authentication provider is configured.")
                        .version("0.1.0"));
    }
}

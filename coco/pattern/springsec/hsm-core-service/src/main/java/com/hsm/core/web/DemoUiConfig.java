package com.hsm.core.web;

import com.hsm.core.config.HsmProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Mounts the demo UI's static assets and redirects "/" to it. Ported from
 * app/main.py's create_app(): the UI lives under the same root the API does
 * (e.g. /api/sensec/hsm), not bare "/" -- so it works correctly behind an Istio
 * route that forwards a path prefix without rewriting it. api-v1-prefix's last
 * segment (".../v1") is the API's own versioning, not part of the service's
 * external root, so the UI mounts one level up from it. Only active when
 * demo-mode=true.
 */
@Configuration
@ConditionalOnProperty(prefix = "hsm", name = "demo-mode", havingValue = "true")
public class DemoUiConfig implements WebMvcConfigurer {

    private final String uiRoot;

    public DemoUiConfig(HsmProperties properties) {
        String prefix = properties.service().apiV1Prefix();
        int lastSlash = prefix.lastIndexOf('/');
        this.uiRoot = lastSlash > 0 ? prefix.substring(0, lastSlash) : "/";
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler(uiRoot + "/**")
                .addResourceLocations("classpath:/static/");
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", uiRoot + "/");
        registry.addViewController(uiRoot + "/").setViewName("forward:" + uiRoot + "/index.html");
    }
}

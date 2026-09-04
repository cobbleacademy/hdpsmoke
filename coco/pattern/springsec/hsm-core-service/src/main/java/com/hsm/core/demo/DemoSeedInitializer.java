package com.hsm.core.demo;

import com.hsm.core.auth.MockJwtValidator;
import com.hsm.core.model.AppGrant;
import com.hsm.core.model.AppRegistration;
import com.hsm.core.repository.AppGrantRepository;
import com.hsm.core.repository.AppRegistrationRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

/**
 * Seeds the demo apps' scopes and the reporting-app -&gt; payments-svc decrypt
 * grant at startup, idempotently. Ported from the demo_mode branch of
 * app/dependencies.py's init_dependencies.
 */
@Component
@ConditionalOnProperty(prefix = "hsm", name = "demo-mode", havingValue = "true")
public class DemoSeedInitializer {

    private final AppRegistrationRepository registrationRepository;
    private final AppGrantRepository grantRepository;

    public DemoSeedInitializer(AppRegistrationRepository registrationRepository, AppGrantRepository grantRepository) {
        this.registrationRepository = registrationRepository;
        this.grantRepository = grantRepository;
    }

    @PostConstruct
    @Transactional
    public void seed() {
        for (Map.Entry<String, List<String>> entry : MockJwtValidator.DEMO_SCOPES.entrySet()) {
            String appId = entry.getKey();
            if (registrationRepository.findById(appId).isEmpty()) {
                registrationRepository.save(new AppRegistration(appId, String.join(",", entry.getValue()), "Seeded demo app", true));
            }
        }
        for (Map.Entry<String, String> grant : MockJwtValidator.DEMO_GRANTS) {
            AppGrant.Key key = new AppGrant.Key(grant.getKey(), grant.getValue(), "decrypt");
            if (!grantRepository.existsById(key)) {
                grantRepository.save(new AppGrant(grant.getKey(), grant.getValue(), "decrypt"));
            }
        }
    }
}

package com.hsm.core.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.core.model.DevStatusItem;
import com.hsm.core.repository.DevStatusItemRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Seeds the Development Status tab's initial rows from dev-status-seed.json
 * (a classpath resource, not under static/ -- it's a one-time seed source,
 * not something the UI fetches directly) the first time the table is empty.
 * Once a row exists in the DB, edits made through DevStatusController are the
 * source of truth; this never re-seeds or overwrites existing rows.
 */
@Component
@ConditionalOnProperty(prefix = "hsm", name = "demo-mode", havingValue = "true")
public class DevStatusSeedInitializer {

    private final DevStatusItemRepository repository;
    private final ObjectMapper objectMapper;

    public DevStatusSeedInitializer(DevStatusItemRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    private record SeedFile(List<SeedItem> items) {
    }

    private record SeedItem(String category, String item, String status, String notes) {
    }

    @PostConstruct
    @Transactional
    public void seed() throws IOException {
        if (repository.count() > 0) {
            return;
        }
        try (InputStream in = new ClassPathResource("dev-status-seed.json").getInputStream()) {
            SeedFile seedFile = objectMapper.readValue(in, SeedFile.class);
            for (SeedItem seedItem : seedFile.items()) {
                repository.save(new DevStatusItem(seedItem.category(), seedItem.item(), seedItem.status(), seedItem.notes()));
            }
        }
    }
}

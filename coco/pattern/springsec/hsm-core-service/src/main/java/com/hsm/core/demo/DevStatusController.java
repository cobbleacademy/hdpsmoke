package com.hsm.core.demo;

import com.hsm.core.dto.DevStatusCreateRequest;
import com.hsm.core.dto.DevStatusItemResponse;
import com.hsm.core.dto.DevStatusUpdateRequest;
import com.hsm.core.model.DevStatusItem;
import com.hsm.core.repository.DevStatusItemRepository;
import com.hsm.core.web.ApiException;
import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * Demo-only. Backs the Development Status tab -- N/P/C rows are read from and
 * written to the DB (dev_status_items), so edits made in the UI survive a
 * restart instead of resetting to a bundled static file.
 */
@RestController
@ConditionalOnProperty(prefix = "hsm", name = "demo-mode", havingValue = "true")
public class DevStatusController {

    private final DevStatusItemRepository repository;

    public DevStatusController(DevStatusItemRepository repository) {
        this.repository = repository;
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/dev-status")
    public Map<String, Object> list() {
        List<DevStatusItemResponse> items = repository.findAllByOrderByCategoryAscIdAsc().stream()
                .map(this::toResponse).toList();
        return Map.of("items", items);
    }

    @PostMapping("${hsm.service.api-v1-prefix}/demo/dev-status")
    public ResponseEntity<DevStatusItemResponse> create(@Valid @RequestBody DevStatusCreateRequest body) {
        DevStatusItem saved = repository.save(new DevStatusItem(body.category(), body.item(), body.status(), body.notes()));
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(saved));
    }

    @PutMapping("${hsm.service.api-v1-prefix}/demo/dev-status/{id}")
    public DevStatusItemResponse update(@PathVariable Long id, @Valid @RequestBody DevStatusUpdateRequest body) {
        DevStatusItem existing = repository.findById(id)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Development status item not found"));
        existing.setCategory(body.category());
        existing.setItem(body.item());
        existing.setStatus(body.status());
        existing.setNotes(body.notes());
        existing.setUpdatedAt(OffsetDateTime.now());
        return toResponse(repository.save(existing));
    }

    @DeleteMapping("${hsm.service.api-v1-prefix}/demo/dev-status/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        if (!repository.existsById(id)) {
            throw new ApiException(HttpStatus.NOT_FOUND, "Development status item not found");
        }
        repository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    private DevStatusItemResponse toResponse(DevStatusItem item) {
        String updatedAt = item.getUpdatedAt() != null ? item.getUpdatedAt().toString() : null;
        return new DevStatusItemResponse(item.getId(), item.getCategory(), item.getItem(), item.getStatus(), item.getNotes(), updatedAt);
    }
}

package com.hsm.bulk.scheduler;

import com.hsm.bulk.config.HsmBulkProperties;
import com.hsm.bulk.service.RotationService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

/**
 * Mirrors com.hsm.core.scheduler.NamedDekRotationScheduler's exact shape -- same
 * @PostConstruct + CronTrigger pattern, same demo-mode guard, same
 * never-propagate-exceptions job body (a failed sweep just waits for the next
 * scheduled fire).
 */
@Component
public class NamedDekRotationScheduler {

    private static final Logger log = LoggerFactory.getLogger(NamedDekRotationScheduler.class);

    private final RotationService rotationService;
    private final HsmBulkProperties properties;
    private final TaskScheduler taskScheduler;

    public NamedDekRotationScheduler(RotationService rotationService, HsmBulkProperties properties, TaskScheduler taskScheduler) {
        this.rotationService = rotationService;
        this.properties = properties;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void start() {
        if (properties.demoMode() || properties.namedDekRotation() == null || !properties.namedDekRotation().enabled()) {
            return;
        }
        String cronExpr = properties.namedDekRotation().cron();
        taskScheduler.schedule(this::runJob, new CronTrigger(toSpringCron(cronExpr)));
        log.info("bulk_named_dek_rotation_scheduler_started cron={} max_age_hours={}", cronExpr, properties.namedDekRotation().maxAgeHours());
    }

    private void runJob() {
        log.info("bulk_named_dek_rotation_job_triggered");
        try {
            int rotated = rotationService.rotateNamedDeks(properties.namedDekRotation().maxAgeHours());
            log.info("bulk_named_dek_rotation_job_completed records={}", rotated);
        } catch (Exception e) {
            log.error("bulk_named_dek_rotation_job_failed error={}", e.getMessage(), e);
        }
    }

    /** Same 5-field-Unix-cron -> Spring's 6-field CronTrigger conversion as hsm-core-service's scheduler. */
    private static String toSpringCron(String cronExpr) {
        String[] fields = cronExpr.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "NAMED_DEK_ROTATION_CRON must have exactly 5 fields (minute hour day month day-of-week), got: " + cronExpr);
        }
        return "0 " + cronExpr.trim();
    }
}

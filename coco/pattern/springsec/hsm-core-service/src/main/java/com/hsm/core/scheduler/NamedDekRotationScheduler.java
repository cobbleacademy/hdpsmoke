package com.hsm.core.scheduler;

import com.hsm.core.config.HsmProperties;
import com.hsm.core.service.RotationService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

/**
 * Mirrors KekRotationScheduler's exact shape -- same @PostConstruct + CronTrigger
 * pattern, same demo-mode guard, same never-propagate-exceptions job body (a failed
 * sweep just waits for the next scheduled fire). Rotates named DEKs
 * (RotationService.rotateNamedDeks) by age, independent of KEK rotation -- see
 * HsmProperties.NamedDekRotation's javadoc for why age, not usage count.
 */
@Component
public class NamedDekRotationScheduler {

    private static final Logger log = LoggerFactory.getLogger(NamedDekRotationScheduler.class);

    private final RotationService rotationService;
    private final HsmProperties properties;
    private final TaskScheduler taskScheduler;

    public NamedDekRotationScheduler(RotationService rotationService, HsmProperties properties, TaskScheduler taskScheduler) {
        this.rotationService = rotationService;
        this.properties = properties;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void start() {
        if (properties.demoMode() || !properties.namedDekRotation().enabled()) {
            return;
        }
        String cronExpr = properties.namedDekRotation().cron();
        taskScheduler.schedule(this::runJob, new CronTrigger(toSpringCron(cronExpr)));
        log.info("named_dek_rotation_scheduler_started cron={} max_age_hours={}", cronExpr, properties.namedDekRotation().maxAgeHours());
    }

    private void runJob() {
        log.info("named_dek_rotation_job_triggered");
        try {
            int rotated = rotationService.rotateNamedDeks(properties.namedDekRotation().maxAgeHours());
            log.info("named_dek_rotation_job_completed records={}", rotated);
        } catch (Exception e) {
            log.error("named_dek_rotation_job_failed error={}", e.getMessage(), e);
        }
    }

    /** Same 5-field-Unix-cron -> Spring's 6-field CronTrigger conversion as KekRotationScheduler. */
    private static String toSpringCron(String cronExpr) {
        String[] fields = cronExpr.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "NAMED_DEK_ROTATION_CRON must have exactly 5 fields (minute hour day month day-of-week), got: " + cronExpr);
        }
        return "0 " + cronExpr.trim();
    }
}

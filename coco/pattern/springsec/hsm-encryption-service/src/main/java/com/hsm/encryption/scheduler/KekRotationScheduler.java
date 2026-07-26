package com.hsm.encryption.scheduler;

import com.hsm.encryption.config.HsmProperties;
import com.hsm.encryption.dto.RotateKekResponse;
import com.hsm.encryption.service.RotationService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

/**
 * Ported from scheduler/kek_rotation_job.py. Registered only when
 * kek-rotation.enabled and not demo-mode, mirroring
 * {@code if settings.kek_rotation_enabled and not settings.demo_mode:} in
 * app/main.py's lifespan. The job body never propagates exceptions -- a failed
 * rotation just waits for the next scheduled fire, same as the Python job.
 */
@Component
public class KekRotationScheduler {

    private static final Logger log = LoggerFactory.getLogger(KekRotationScheduler.class);

    private final RotationService rotationService;
    private final HsmProperties properties;
    private final TaskScheduler taskScheduler;

    public KekRotationScheduler(RotationService rotationService, HsmProperties properties, TaskScheduler taskScheduler) {
        this.rotationService = rotationService;
        this.properties = properties;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void start() {
        if (properties.demoMode() || !properties.kekRotation().enabled()) {
            return;
        }
        String cronExpr = properties.kekRotation().cron();
        taskScheduler.schedule(this::runJob, new CronTrigger(toSpringCron(cronExpr)));
        log.info("kek_rotation_scheduler_started cron={}", cronExpr);
    }

    private void runJob() {
        log.info("kek_rotation_job_triggered");
        try {
            RotateKekResponse result = rotationService.rotateKek("scheduler");
            log.info("kek_rotation_job_completed records={}", result.recordsQueued());
        } catch (Exception e) {
            log.error("kek_rotation_job_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * Python's cron_expr is standard 5-field Unix cron (minute hour day month
     * day-of-week, no seconds). Spring's CronTrigger uses 6 fields (seconds
     * first), so prepend seconds=0.
     */
    private static String toSpringCron(String cronExpr) {
        String[] fields = cronExpr.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "KEK_ROTATION_CRON must have exactly 5 fields (minute hour day month day-of-week), got: " + cronExpr);
        }
        return "0 " + cronExpr.trim();
    }
}

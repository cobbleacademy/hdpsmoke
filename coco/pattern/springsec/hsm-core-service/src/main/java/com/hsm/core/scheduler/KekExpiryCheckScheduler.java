package com.hsm.core.scheduler;

import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.KekStatus;
import com.hsm.core.service.KekExpiryMonitor;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Logs the expiry state of every KEK in use: once shortly after startup (so every
 * deploy shows it) and then on hsm.kek-expiry.cron (daily 06:00 by default). One line
 * per KEK, at a level an alert can key on:
 * <pre>
 * INFO  kek_expiry_check kek_name=… version=… state=ok|no_expiry …
 * WARN  kek_expiry_check kek_name=… version=… state=expiring days_left=12 …
 * ERROR kek_expiry_check kek_name=… version=… state=expired|disabled|not_yet_valid …
 * WARN  kek_expiry_check_failed kek_name=… error=…
 * INFO  kek_expiry_check_completed keks=… ok=… expiring=… failing=…
 * </pre>
 * Read-only (one Key Vault GET per KEK), so every pod runs it -- no lock. Not in demo
 * mode, matching the rotation schedulers. The job never throws.
 */
@Component
public class KekExpiryCheckScheduler {

    private static final Logger log = LoggerFactory.getLogger(KekExpiryCheckScheduler.class);
    static final Duration STARTUP_DELAY = Duration.ofSeconds(30);

    private final KekExpiryMonitor monitor;
    private final HsmProperties properties;
    private final TaskScheduler taskScheduler;

    public KekExpiryCheckScheduler(KekExpiryMonitor monitor, HsmProperties properties, TaskScheduler taskScheduler) {
        this.monitor = monitor;
        this.properties = properties;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void start() {
        if (properties.demoMode() || !properties.kekExpiry().enabled()) {
            return;
        }
        String cronExpr = properties.kekExpiry().cron();
        taskScheduler.schedule(this::runCheck, new CronTrigger(KekRotationScheduler.toSpringCron(cronExpr)));
        taskScheduler.schedule(this::runCheck, Instant.now().plus(STARTUP_DELAY));
        log.info("kek_expiry_check_scheduler_started cron={} warn_days={}", cronExpr, monitor.warnDays());
    }

    /** Package-private for tests. */
    void runCheck() {
        try {
            KekExpiryMonitor.Snapshot snapshot = monitor.refresh();
            Instant now = snapshot.takenAt();
            int warnDays = monitor.warnDays();
            int ok = 0;
            int expiring = 0;
            int failing = 0;
            for (KekExpiryMonitor.Check check : snapshot.checks()) {
                if (check.status() == null) {
                    failing++;
                    log.warn("kek_expiry_check_failed kek_name={} error={}", check.kekName(), check.error());
                    continue;
                }
                KekStatus status = check.status();
                KekStatus.State state = status.state(now, warnDays);
                String line = "kek_expiry_check kek_name={} version={} state={} expires_on={} days_left={} enabled={} not_before={}";
                Object[] args = {check.kekName(), status.version(), state.label(), status.expiresOn(),
                        status.daysLeft(now), status.enabled(), status.notBefore()};
                switch (state) {
                    case OK, NO_EXPIRY -> {
                        ok++;
                        log.info(line, args);
                    }
                    case EXPIRING -> {
                        expiring++;
                        log.warn(line, args);
                    }
                    default -> {
                        failing++;
                        log.error(line, args);
                    }
                }
            }
            log.info("kek_expiry_check_completed keks={} ok={} expiring={} failing={} warn_days={}",
                    snapshot.checks().size(), ok, expiring, failing, warnDays);
        } catch (Exception e) {
            log.error("kek_expiry_check_job_failed error={}", e.getMessage(), e);
        }
    }
}

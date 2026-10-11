package com.hsm.chassis.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A {@link JobLock} for any service on the chassis that has a database: the
 * cross-pod Postgres advisory lock when connected to Postgres, an in-JVM lock
 * otherwise (the demo profile's H2). Chosen from the database actually connected
 * to, not from a profile -- tests run the demo profile against H2, and a
 * Postgres-backed demo still wants the cross-pod lock.
 *
 * <p>Services without a DataSource get no JobLock; a service that defines its own
 * JobLock bean keeps it.
 */
@AutoConfiguration(afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@ConditionalOnSingleCandidate(DataSource.class)
public class ChassisJobLockAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ChassisJobLockAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public JobLock jobLock(DataSource dataSource) throws SQLException {
        String product;
        try (Connection connection = dataSource.getConnection()) {
            product = connection.getMetaData().getDatabaseProductName();
        }
        if ("PostgreSQL".equalsIgnoreCase(product)) {
            log.info("job_lock_mode=postgres_advisory");
            return new PostgresAdvisoryJobLock(dataSource);
        }
        log.warn("job_lock_mode=in_process database={} -- single-runner guarantee covers this JVM only", product);
        return new InProcessJobLock();
    }
}

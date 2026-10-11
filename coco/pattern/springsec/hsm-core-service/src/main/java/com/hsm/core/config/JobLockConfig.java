package com.hsm.core.config;

import com.hsm.core.lock.InProcessJobLock;
import com.hsm.core.lock.JobLock;
import com.hsm.core.lock.PostgresAdvisoryJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Picks the {@link JobLock} implementation from the database actually
 * connected to, not from demo-mode -- tests run the demo profile against H2,
 * and a Postgres-backed demo would still want the cross-pod lock.
 */
@Configuration
public class JobLockConfig {

    private static final Logger log = LoggerFactory.getLogger(JobLockConfig.class);

    @Bean
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

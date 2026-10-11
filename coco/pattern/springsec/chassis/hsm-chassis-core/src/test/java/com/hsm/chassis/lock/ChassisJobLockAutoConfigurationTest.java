package com.hsm.chassis.lock;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ChassisJobLockAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ChassisJobLockAutoConfiguration.class));

    private static DataSource h2() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:joblock-" + System.nanoTime() + ";DB_CLOSE_DELAY=-1");
        return ds;
    }

    @Test
    void nonPostgresDatabaseGetsTheInProcessLock() {
        runner.withBean(DataSource.class, ChassisJobLockAutoConfigurationTest::h2)
                .run(context -> assertThat(context).getBean(JobLock.class).isInstanceOf(InProcessJobLock.class));
    }

    @Test
    void serviceWithoutADatabaseGetsNoLock() {
        runner.run(context -> assertThat(context).doesNotHaveBean(JobLock.class));
    }

    @Test
    void serviceDefinedLockIsKept() {
        JobLock own = new JobLock() {
            @Override
            public <T> Optional<T> tryRun(String key, java.util.function.Supplier<T> work) {
                return Optional.empty();
            }
        };
        runner.withBean(DataSource.class, ChassisJobLockAutoConfigurationTest::h2)
                .withBean(JobLock.class, () -> own)
                .run(context -> assertThat(context).getBean(JobLock.class).isSameAs(own));
    }
}

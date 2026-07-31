package com.hsm.client;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.db.DbBulkJob;
import com.hsm.client.file.FileBulkJob;
import com.hsm.client.svc.SvcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * One process, one job, one direction, per invocation (client.job.type / client.job.mode) --
 * this is a batch job, not a server (spring.main.web-application-type: none in
 * application.yml). Runs its configured job to completion, then exits with a
 * process exit code reflecting success/failure -- important for this to be
 * schedulable/scriptable (cron, an orchestration pipeline, etc.), unlike SVC which
 * stays up as a long-running HTTP service.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ClientApplication {

    private static final Logger log = LoggerFactory.getLogger(ClientApplication.class);

    static {
        FipsBootstrap.register();
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(ClientApplication.class, args);
        int exitCode = SpringApplication.exit(context);
        System.exit(exitCode);
    }

    @Bean
    public CommandLineRunner run(ClientProperties properties) {
        return args -> {
            SvcClient svcClient = new SvcClient(properties.svc());
            ClientProperties.Job job = properties.job();

            log.info("hsm_bulk_client_start job_type={} job_mode={}", job.type(), job.mode());
            switch (job.type()) {
                case DB -> {
                    DbBulkJob dbJob = new DbBulkJob(properties.db(), properties.svc(), svcClient);
                    if (job.mode() == ClientProperties.Job.Mode.ENCRYPT) {
                        dbJob.encrypt();
                    } else {
                        dbJob.decrypt();
                    }
                }
                case FILE -> {
                    FileBulkJob fileJob = new FileBulkJob(properties.file(), properties.svc(), svcClient);
                    if (job.mode() == ClientProperties.Job.Mode.ENCRYPT) {
                        fileJob.encrypt();
                    } else {
                        fileJob.decrypt();
                    }
                }
            }
            log.info("hsm_bulk_client_complete job_type={} job_mode={}", job.type(), job.mode());
        };
    }
}

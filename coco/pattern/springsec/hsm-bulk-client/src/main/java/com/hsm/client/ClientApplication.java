package com.hsm.client;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.db.DbBulkJob;
import com.hsm.client.deidjob.DeidInitKeyCommand;
import com.hsm.client.deidjob.DeidJob;
import com.hsm.client.file.FileBulkJob;
import com.hsm.client.svc.SvcClient;
import com.hsm.crypto.FipsBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

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
            List<String> argList = Arrays.asList(args);
            if (argList.contains("--deid.init-key")) {
                // A one-off command, whatever client.job.type says: issue a named DEK, print its edek_id.
                new DeidInitKeyCommand(new SvcClient(properties.svc())).run(argValue(argList, "--name"));
                return;
            }
            ClientProperties.Job job = properties.job();
            if (job.type() == ClientProperties.Job.Type.DEID) {
                // HSM and database connections are opened only if the profile needs them.
                log.info("hsm_bulk_client_start job_type=DEID");
                DeidJob.create(withDeidArgs(properties, argList)).run();
                log.info("hsm_bulk_client_complete job_type=DEID");
                return;
            }
            SvcClient svcClient = new SvcClient(properties.svc());

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
                case DEID -> throw new IllegalStateException("handled above");
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

    /**
     * {@code --dry-run} and {@code --sample N} (or {@code --sample=N}) on the command line
     * override client.deid.dry-run / client.deid.sample.
     */
    static ClientProperties withDeidArgs(ClientProperties p, List<String> args) {
        ClientProperties.Deid d = p.deid();
        if (d == null) {
            d = new ClientProperties.Deid(null, null, null, null, false, 0, null);
        }
        boolean dryRun = d.dryRun() || args.contains("--dry-run");
        int sample = d.sample();
        String s = argValue(args, "--sample");
        if (s != null) {
            try {
                sample = Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("--sample must be a number");
            }
        }
        ClientProperties.Deid deid = new ClientProperties.Deid(d.profile(), d.source(), d.output(), d.surrogate(),
                dryRun, sample, d.triggeredBy());
        return new ClientProperties(p.job(), p.svc(), p.db(), p.file(), deid);
    }

    /** The value of {@code --name value} or {@code --name=value}, or null. */
    static String argValue(List<String> args, String option) {
        for (int i = 0; i < args.size(); i++) {
            String a = args.get(i);
            if (a.startsWith(option + "=")) {
                return a.substring(option.length() + 1);
            }
            if (a.equals(option) && i + 1 < args.size() && !args.get(i + 1).startsWith("--")) {
                return args.get(i + 1);
            }
        }
        return null;
    }
}

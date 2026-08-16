package com.hsm.core.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Cross-cutting call timing for the HSM-bound collaborators that sit behind
 * an interface implemented by a distinct Spring bean (proxy-based AOP can
 * only intercept calls that cross a bean boundary -- it cannot see private
 * methods or self-invocation within a single class, e.g.
 * EncryptionService.resolveDek() or the static DekManager utility, which is
 * why those two are timed manually instead of covered here).
 */
@Aspect
@Component
public class ComponentTimingAspect {

    private static final Logger log = LoggerFactory.getLogger(ComponentTimingAspect.class);

    @Around("execution(* com.hsm.core.auth.PbacClient.check(..))")
    public Object timePbacCheck(ProceedingJoinPoint pjp) throws Throwable {
        return time(pjp, "PbacClient");
    }

    @Around("execution(* com.hsm.core.crypto.KekClient.wrapDek(..))")
    public Object timeKekWrap(ProceedingJoinPoint pjp) throws Throwable {
        return time(pjp, "KekClient");
    }

    @Around("execution(* com.hsm.core.crypto.KekClient.unwrapDek(..))")
    public Object timeKekUnwrap(ProceedingJoinPoint pjp) throws Throwable {
        return time(pjp, "KekClient");
    }

    @Around("execution(* com.hsm.core.repository.EdekRecordRepository.save(..))")
    public Object timeEdekSave(ProceedingJoinPoint pjp) throws Throwable {
        return time(pjp, "EdekRecordRepository");
    }

    private Object time(ProceedingJoinPoint pjp, String component) throws Throwable {
        String method = pjp.getSignature().getName();
        long start = System.nanoTime();
        log.info("component_call_started component={} method={}", component, method);
        try {
            Object result = pjp.proceed();
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            log.info("component_call_completed component={} method={} duration_ms={} status=success",
                    component, method, durationMs);
            return result;
        } catch (Throwable t) {
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            log.info("component_call_completed component={} method={} duration_ms={} status=error",
                    component, method, durationMs);
            throw t;
        }
    }
}

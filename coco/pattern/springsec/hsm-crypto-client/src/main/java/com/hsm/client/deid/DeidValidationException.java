package com.hsm.client.deid;

import java.util.List;

/**
 * A profile can't run: every problem found is listed, so a reviewer fixes them in one
 * pass. Thrown before any record is read (DEIDENTIFICATION.md principle 3).
 */
public class DeidValidationException extends RuntimeException {

    private final List<String> problems;

    public DeidValidationException(List<String> problems) {
        super("profile is not valid:\n  - " + String.join("\n  - ", problems));
        this.problems = List.copyOf(problems);
    }

    public List<String> problems() {
        return problems;
    }
}

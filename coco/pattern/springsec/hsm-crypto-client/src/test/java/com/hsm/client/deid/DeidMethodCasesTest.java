package com.hsm.client.deid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.deid.transform.GeneralizeTransform;
import com.hsm.client.deid.transform.MaskTransform;
import com.hsm.client.deid.transform.Normalize;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The edge cases in {@code deid-method-cases.json}, which the Python implementation
 * ({@code hsm-databricks-udf}, {@code tests/deid/test_method_cases.py}) checks too, so
 * a Databricks export masks and generalizes exactly as this one (DEIDENTIFICATION.md
 * section 17.2). A failure here means this code's behaviour changed: every vendor
 * file would change with it.
 */
class DeidMethodCasesTest {

    @Test
    void everyCaseGivesTheRecordedOutput() throws Exception {
        JsonNode doc;
        try (InputStream in = getClass().getResourceAsStream("/deid-method-cases.json")) {
            doc = new ObjectMapper().readTree(in);
        }
        ObjectMapper om = new ObjectMapper();
        Clock clock = Clock.fixed(Instant.parse(doc.get("clock").asText()), ZoneOffset.UTC);
        List<String> wrong = new ArrayList<>();
        int n = 0;
        for (JsonNode c : doc.get("cases")) {
            n++;
            @SuppressWarnings("unchecked")
            Map<String, Object> options = om.convertValue(c.get("options"), Map.class);
            String value = c.get("value").asText();
            String out;
            String err = null;
            try {
                out = switch (c.get("method").asText()) {
                    case "generalize" -> GeneralizeTransform.create(new FieldOptions(options), clock,
                            TransformContext.defaultRestrictedZip3()).apply(value, Map.of(), new FieldStats());
                    case "mask" -> MaskTransform.create(new FieldOptions(options)).apply(value, Map.of(), new FieldStats());
                    default -> Normalize.fromLabel((String) options.get("normalize")).apply(value);
                };
            } catch (RuntimeException e) {
                out = null;
                err = e.getMessage();
            }
            String expectedOut = c.has("out") ? c.get("out").asText() : null;
            String expectedErr = c.has("err") ? c.get("err").asText() : null;
            if (!java.util.Objects.equals(expectedOut, out) || !java.util.Objects.equals(expectedErr, err)) {
                wrong.add(c.get("method").asText() + " " + options + " '" + value + "': expected "
                        + (expectedErr != null ? "error " + expectedErr : expectedOut) + ", got "
                        + (err != null ? "error " + err : out));
            }
        }
        assertTrue(n > 150, "the case file is loaded");
        assertEquals(List.of(), wrong);
    }
}

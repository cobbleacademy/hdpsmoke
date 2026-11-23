package com.hsm.client.deidjob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.deid.DeidProfile;
import com.hsm.client.deid.DeidValidationException;
import com.hsm.client.deid.ProfileValidator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The profiles in {@code hsm-crypto-client/src/test/resources/deid-profile-cases.json},
 * which the Python implementation ({@code hsm-databricks-udf},
 * {@code tests/deid/test_profile_cases.py}) checks too: both read and refuse the same
 * profiles with the same messages (DEIDENTIFICATION.md section 17.2). Here, not in
 * hsm-crypto-client, because the YAML is read as the job reads it ({@link DeidJob#parseProfile}).
 */
class DeidProfileCasesTest {

    @Test
    void everyProfileGivesTheRecordedResult() throws Exception {
        ObjectMapper om = new ObjectMapper();
        JsonNode doc = om.readTree(Files.readString(
                Path.of("../hsm-crypto-client/src/test/resources/deid-profile-cases.json"), StandardCharsets.UTF_8));
        List<String> wrong = new ArrayList<>();
        int n = 0;
        for (JsonNode c : doc.get("cases")) {
            n++;
            List<String> columns = c.get("columns").isNull() ? null : om.convertValue(c.get("columns"), List.class);
            List<String> problems = null;
            List<String> outputs = null;
            List<String> structural = null;
            try {
                DeidProfile p = DeidJob.parseProfile(c.get("yaml").asText().getBytes(StandardCharsets.UTF_8));
                problems = ProfileValidator.problems(p, columns);
                outputs = p.fields().stream().map(p::outputName).toList();
            } catch (DeidValidationException e) {
                structural = e.problems();
            }
            if (!Objects.equals(list(om, c.get("problems")), problems) || !Objects.equals(list(om, c.get("outputs")), outputs)
                    || !Objects.equals(list(om, c.get("structural")), structural)) {
                wrong.add("case " + n + ": expected problems=" + c.get("problems") + " structural=" + c.get("structural")
                        + ", got problems=" + problems + " structural=" + structural);
            }
        }
        assertTrue(n > 100, "the case file is loaded");
        assertEquals(List.of(), wrong);
    }

    @SuppressWarnings("unchecked")
    private static List<String> list(ObjectMapper om, JsonNode node) {
        return node == null ? null : om.convertValue(node, List.class);
    }
}

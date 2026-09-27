package com.hsm.fileservice.web;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestPathsAndCallerTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "/abs", "a//b", "a/../b", "..", "a/./b", "a\\b", "trailing/", "a/\u0000b"})
    void badPaths_areRejected(String path) {
        FileServiceException e = assertThrows(FileServiceException.class, () -> RequestPaths.validate(path));
        assertEquals(ErrorCode.BAD_PATH, e.code());
    }

    @Test
    void overLongPath_isRejected() {
        assertThrows(FileServiceException.class, () -> RequestPaths.validate("a".repeat(RequestPaths.MAX_LENGTH + 1)));
    }

    @Test
    void bookkeepingPaths_areNotFound() {
        FileServiceException e = assertThrows(FileServiceException.class,
                () -> RequestPaths.validate(".hsm_bulk_results/job/batch-000001.jsonl"));
        assertEquals(ErrorCode.NOT_FOUND, e.code());
    }

    @Test
    void goodPaths_pass() {
        assertEquals("tenant-a/2026/report v2+final.pdf", RequestPaths.validate("tenant-a/2026/report v2+final.pdf"));
    }

    @Test
    void prefixes_matchOnSegmentBoundary() {
        List<String> allowed = List.of("tenant-a", "shared/docs/");
        assertTrue(RequestPaths.isAllowed("tenant-a/x.pdf", allowed));
        assertTrue(RequestPaths.isAllowed("shared/docs/x.pdf", allowed));
        assertFalse(RequestPaths.isAllowed("tenant-ab/x.pdf", allowed));
        assertFalse(RequestPaths.isAllowed("shared/docsx/y.pdf", allowed));
        assertTrue(RequestPaths.isAllowed("anything/at/all", List.of("*")));
    }

    @Test
    void xfcc_takesTheImmediatePeersUri() {
        String single = "By=spiffe://cluster.local/ns/app/sa/file-svc;Hash=abc;Subject=\"\";URI=spiffe://cluster.local/ns/app/sa/bff";
        assertEquals("spiffe://cluster.local/ns/app/sa/bff", CallerIdentity.immediatePeerSpiffeId(single));
        String chain = "By=x;URI=spiffe://cluster.local/ns/gw/sa/ingress,By=y;Subject=\"CN=a,O=b\";URI=spiffe://cluster.local/ns/app/sa/bff";
        assertEquals("spiffe://cluster.local/ns/app/sa/bff", CallerIdentity.immediatePeerSpiffeId(chain));
        assertNull(CallerIdentity.immediatePeerSpiffeId(null));
        assertNull(CallerIdentity.immediatePeerSpiffeId("By=x;Hash=y"));
    }
}

package com.hsm.filestore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalFileStoreTest {

    @TempDir
    Path root;

    @Test
    void size_reportsStoredBytes() throws Exception {
        Files.createDirectories(root.resolve("a"));
        Files.write(root.resolve("a/f.bin"), new byte[123]);
        assertEquals(123, new LocalFileStore(root.toString()).size("a/f.bin"));
    }

    @Test
    void size_missingFile_isStoreFileNotFound() {
        assertThrows(StoreFileNotFoundException.class, () -> new LocalFileStore(root.toString()).size("nope.bin"));
    }

    @Test
    void pathsEscapingRoot_areRefused() throws Exception {
        Path outside = Files.createTempFile("outside", ".bin");
        try {
            LocalFileStore store = new LocalFileStore(root.resolve("inner").toString());
            Files.createDirectories(root.resolve("inner"));
            String escape = "../../" + outside.getFileName();
            assertThrows(IllegalArgumentException.class, () -> store.openRead(escape));
            assertThrows(IllegalArgumentException.class, () -> store.size("../x"));
            assertThrows(IllegalArgumentException.class, () -> store.openWrite("../x"));
        } finally {
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void list_excludesCheckpointAndResultsDirectories() throws Exception {
        Files.createDirectories(root.resolve(FileStore.MANIFEST_DIR));
        Files.createDirectories(root.resolve(FileStore.RESULTS_DIR + "/job1"));
        Files.write(root.resolve(FileStore.MANIFEST_DIR + "/manifest.json"), new byte[1]);
        Files.write(root.resolve(FileStore.RESULTS_DIR + "/job1/batch-0.jsonl"), new byte[1]);
        Files.write(root.resolve("data.bin"), new byte[1]);
        assertEquals(List.of("data.bin"), new LocalFileStore(root.toString()).list(List.of()));
    }
}

package com.hsm.client.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Local-disk FileStore, rooted at a fixed directory. */
public class LocalFileStore implements FileStore {

    private final Path root;

    public LocalFileStore(String root) {
        this.root = Path.of(root).toAbsolutePath().normalize();
    }

    @Override
    public List<String> list(List<String> fileTypes) {
        try (Stream<Path> walk = Files.walk(root)) {
            return walk
                    .filter(Files::isRegularFile)
                    .filter(p -> matchesType(p, fileTypes))
                    .map(p -> root.relativize(p).toString().replace('\\', '/'))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list files under " + root, e);
        }
    }

    @Override
    public InputStream openRead(String relativePath) {
        try {
            return Files.newInputStream(root.resolve(relativePath));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open " + relativePath + " for read under " + root, e);
        }
    }

    @Override
    public OutputStream openWrite(String relativePath) {
        try {
            Path target = root.resolve(relativePath);
            Files.createDirectories(target.getParent());
            return Files.newOutputStream(target);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open " + relativePath + " for write under " + root, e);
        }
    }

    private static boolean matchesType(Path p, List<String> fileTypes) {
        if (fileTypes == null || fileTypes.isEmpty()) {
            return true;
        }
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        for (String type : fileTypes) {
            if (name.endsWith(type.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }
}

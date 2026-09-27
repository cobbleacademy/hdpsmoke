package com.hsm.client.fileformat;

import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Shared helpers: encrypt/decrypt in memory, and split an encrypted file into header + frames for tamper tests. */
final class FileFormatTestSupport {

    static {
        FipsBootstrap.register();
    }

    static final String OWNER = "payments-svc";

    private FileFormatTestSupport() {
    }

    static byte[] randomBytes(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    static byte[] newDek() {
        return DekManager.generateDek();
    }

    static Encrypted encrypt(byte[] plaintext, UUID edekId, byte[] dek, EncryptedFileWriter.Options options) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileWriter.Result result = EncryptedFileWriter.write(
                new ByteArrayInputStream(plaintext), out, edekId, dek, OWNER, options);
        return new Encrypted(out.toByteArray(), result);
    }

    static byte[] decrypt(byte[] file, byte[] dek) {
        return decrypt(file, dek, EncryptedFileReader.Limits.DEFAULT);
    }

    static byte[] decrypt(byte[] file, byte[] dek, EncryptedFileReader.Limits limits) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileReader.open(new ByteArrayInputStream(file), limits).decryptTo(out, dek, OWNER);
        return out.toByteArray();
    }

    record Encrypted(byte[] bytes, EncryptedFileWriter.Result result) {
    }

    /** Header bytes plus each frame as stored (length prefix included), so tests can drop/reorder/splice frames. */
    record Split(byte[] header, List<byte[]> frames) {
        byte[] join() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.writeBytes(header);
            frames.forEach(out::writeBytes);
            return out.toByteArray();
        }

        Split withFrames(List<byte[]> newFrames) {
            return new Split(header, newFrames);
        }

        Split withHeader(byte[] newHeader) {
            return new Split(newHeader, frames);
        }
    }

    static Split split(byte[] file, int headerBytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(file))) {
            byte[] header = in.readNBytes(headerBytes);
            List<byte[]> frames = new ArrayList<>();
            while (in.available() > 0) {
                int len = in.readInt();
                byte[] frame = new byte[4 + len];
                frame[0] = (byte) (len >>> 24);
                frame[1] = (byte) (len >>> 16);
                frame[2] = (byte) (len >>> 8);
                frame[3] = (byte) len;
                in.readFully(frame, 4, len);
                frames.add(frame);
            }
            return new Split(header, frames);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}

package com.hsm.client.fileformat;

import com.hsm.client.crypto.DekManager;
import com.hsm.client.fileformat.EncryptedFileException.Reason;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;
import com.hsm.client.fileformat.FileFormatTestSupport.Encrypted;
import com.hsm.client.fileformat.FileFormatTestSupport.Split;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static com.hsm.client.fileformat.FileFormatTestSupport.OWNER;
import static com.hsm.client.fileformat.FileFormatTestSupport.concat;
import static com.hsm.client.fileformat.FileFormatTestSupport.decrypt;
import static com.hsm.client.fileformat.FileFormatTestSupport.encrypt;
import static com.hsm.client.fileformat.FileFormatTestSupport.newDek;
import static com.hsm.client.fileformat.FileFormatTestSupport.randomBytes;
import static com.hsm.client.fileformat.FileFormatTestSupport.split;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round trips for v1 and v2, and the tamper cases v2 exists to catch. Every tamper
 * case is built from real encrypted output by moving whole frames around -- no
 * ciphertext is forged -- which is exactly what an attacker with storage write
 * access but no key can do.
 */
class EncryptedFileFormatTest {

    private static final int CHUNK = 4096;
    private static final UUID EDEK = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");

    // ---- round trips ----

    @ParameterizedTest
    @ValueSource(ints = {0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 3 * CHUNK + 17})
    void roundTrip_allVersionsAndCompression(int size) {
        byte[] dek = newDek();
        byte[] plaintext = randomBytes(size, size);
        for (Version v : Version.values()) {
            for (boolean compress : new boolean[]{false, true}) {
                Encrypted enc = encrypt(plaintext, EDEK, dek, new EncryptedFileWriter.Options(v, CHUNK, compress));
                assertArrayEquals(plaintext, decrypt(enc.bytes(), dek), v + " compress=" + compress + " size=" + size);
                assertEquals(size, enc.result().plaintextBytes());
                assertEquals(enc.bytes().length, enc.result().encryptedBytes());
            }
        }
    }

    @Test
    void v2_emptyFile_isOneFinalChunk_v1_emptyFile_isHeaderOnly() {
        byte[] dek = newDek();
        Encrypted v1 = encrypt(new byte[0], EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, false));
        Encrypted v2 = encrypt(new byte[0], EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        assertEquals(EncryptedFileFormat.V1_HEADER_BYTES, v1.bytes().length);
        assertEquals(0, v1.result().chunkCount());
        assertEquals(1, v2.result().chunkCount());
    }

    @Test
    void chunkCounts_matchSize() {
        byte[] dek = newDek();
        Encrypted exact = encrypt(randomBytes(3 * CHUNK, 1), EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        Encrypted partial = encrypt(randomBytes(3 * CHUNK + 1, 1), EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        assertEquals(3, exact.result().chunkCount());
        assertEquals(4, partial.result().chunkCount());
    }

    @Test
    void v2_header_recordsIdsAndChunkSize() {
        byte[] dek = newDek();
        Encrypted enc = encrypt(randomBytes(100, 2), EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        FileHeader header = EncryptedFileFormat.readHeader(new ByteArrayInputStream(enc.bytes()));
        assertEquals(Version.V2, header.version());
        assertEquals(EDEK, header.edekId());
        assertEquals(enc.result().fileId(), header.fileId());
        assertEquals(4, header.fileId().version(), "file_id must be a random v4 UUID");
        assertEquals(CHUNK, header.chunkSizeBytes());
    }

    @Test
    void fileIds_areFreshPerEncryption_evenForIdenticalInput() {
        byte[] dek = newDek();
        byte[] plaintext = randomBytes(10, 3);
        UUID a = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false)).result().fileId();
        UUID b = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false)).result().fileId();
        assertNotEquals(a, b);
    }

    @Test
    void v1_header_hasNoFileId() {
        Encrypted enc = encrypt(randomBytes(10, 4), EDEK, newDek(), EncryptedFileWriter.Options.v1(CHUNK, false));
        assertNull(enc.result().fileId());
        FileHeader header = EncryptedFileFormat.readHeader(new ByteArrayInputStream(enc.bytes()));
        assertEquals(Version.V1, header.version());
        assertEquals(EDEK, header.edekId());
    }

    // ---- compatibility with the pre-existing v1 reader ----

    /** Verbatim port of FileBulkJob.decryptOneFile as it existed before the codec -- the "old reader" still deployed in the field. */
    private static byte[] legacyV1Read(byte[] file, byte[] dek) throws Exception {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(file));
        in.readLong();
        in.readLong();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            int frameLength;
            try {
                frameLength = in.readInt();
            } catch (EOFException eof) {
                break;
            }
            byte[] frame = new byte[frameLength];
            in.readFully(frame);
            byte[] iv = Arrays.copyOfRange(frame, 0, 12);
            byte[] tag = Arrays.copyOfRange(frame, 12, 28);
            byte[] ct = Arrays.copyOfRange(frame, 28, frame.length);
            byte[] pt = DekManager.decrypt(ct, tag, iv, dek, OWNER);
            byte[] marked = Base64.getDecoder().decode(new String(pt, StandardCharsets.UTF_8));
            byte[] payload = Arrays.copyOfRange(marked, 1, marked.length);
            out.write(marked[0] == 0x01 ? new GZIPInputStream(new ByteArrayInputStream(payload)).readAllBytes() : payload);
        }
        return out.toByteArray();
    }

    @Test
    void newV1Writer_isReadableByOldReader() throws Exception {
        byte[] dek = newDek();
        byte[] plaintext = randomBytes(3 * CHUNK + 5, 5);
        for (boolean compress : new boolean[]{false, true}) {
            Encrypted enc = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, compress));
            assertArrayEquals(plaintext, legacyV1Read(enc.bytes(), dek));
        }
    }

    @Test
    void oldReader_onV2File_resolvesAGarbageEdekId_soItFailsLoudlyNotSilently() {
        // The old reader treats the first 16 bytes as edek_id. For a v2 file those are
        // "HSMF" 0x02 + 11 bytes of the real id, so /dek/unwrap would answer "EDEK not
        // found" -- a loud failure, never wrong plaintext.
        Encrypted enc = encrypt(randomBytes(10, 6), EDEK, newDek(), EncryptedFileWriter.Options.v2(CHUNK, false));
        ByteBuffer buf = ByteBuffer.wrap(enc.bytes(), 0, 16);
        UUID whatOldReaderSees = new UUID(buf.getLong(), buf.getLong());
        assertNotEquals(EDEK, whatOldReaderSees);
    }

    // ---- tamper cases (v2 must catch every one) ----

    private static Split v2Split(byte[] dek, byte[] plaintext) {
        Encrypted enc = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        return split(enc.bytes(), EncryptedFileFormat.V2_HEADER_BYTES);
    }

    private static Reason failureReason(byte[] file, byte[] dek) {
        return assertThrows(EncryptedFileException.class, () -> decrypt(file, dek)).reason();
    }

    @Test
    void droppedFinalChunk_isTruncated() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(3 * CHUNK + 9, 7));
        byte[] tampered = s.withFrames(s.frames().subList(0, s.frames().size() - 1)).join();
        assertEquals(Reason.TRUNCATED, failureReason(tampered, dek));
    }

    @Test
    void droppedFinalChunk_v1_goesUndetected_whichIsWhyV2Exists() {
        byte[] dek = newDek();
        byte[] plaintext = randomBytes(3 * CHUNK + 9, 7);
        Encrypted enc = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, false));
        Split s = split(enc.bytes(), EncryptedFileFormat.V1_HEADER_BYTES);
        byte[] tampered = s.withFrames(s.frames().subList(0, s.frames().size() - 1)).join();
        byte[] result = decrypt(tampered, dek);
        assertEquals(3 * CHUNK, result.length, "v1 silently returns a shorter file");
    }

    @Test
    void cutMidFrame_isTruncated() {
        byte[] dek = newDek();
        byte[] full = v2Split(dek, randomBytes(2 * CHUNK + 1, 8)).join();
        byte[] tampered = Arrays.copyOf(full, full.length - 10);
        assertEquals(Reason.TRUNCATED, failureReason(tampered, dek));
    }

    @Test
    void headerOnly_isTruncated() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(10, 9));
        assertEquals(Reason.TRUNCATED, failureReason(s.withFrames(List.of()).join(), dek));
    }

    @Test
    void reorderedChunks_areOutOfOrder() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(3 * CHUNK + 1, 10));
        List<byte[]> frames = new ArrayList<>(s.frames());
        byte[] first = frames.get(0);
        frames.set(0, frames.get(1));
        frames.set(1, first);
        assertEquals(Reason.CHUNK_OUT_OF_ORDER, failureReason(s.withFrames(frames).join(), dek));
    }

    @Test
    void duplicatedChunk_isOutOfOrder() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(3 * CHUNK + 1, 11));
        List<byte[]> frames = new ArrayList<>(s.frames());
        frames.add(1, frames.get(0));
        assertEquals(Reason.CHUNK_OUT_OF_ORDER, failureReason(s.withFrames(frames).join(), dek));
    }

    @Test
    void chunkSplicedFromAnotherFileUnderTheSameKey_isFileIdMismatch() {
        byte[] dek = newDek(); // same (named) DEK for both files -- the case v1 can't detect
        Split victim = v2Split(dek, randomBytes(3 * CHUNK + 1, 12));
        Split donor = v2Split(dek, randomBytes(3 * CHUNK + 1, 13));
        List<byte[]> frames = new ArrayList<>(victim.frames());
        frames.set(1, donor.frames().get(1));
        assertEquals(Reason.FILE_ID_MISMATCH, failureReason(victim.withFrames(frames).join(), dek));
    }

    @Test
    void v1_chunkSplicedFromAnotherFileUnderTheSameKey_goesUndetected() {
        byte[] dek = newDek();
        Encrypted a = encrypt(randomBytes(2 * CHUNK, 14), EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, false));
        Encrypted b = encrypt(randomBytes(2 * CHUNK, 15), EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, false));
        Split sa = split(a.bytes(), 16);
        Split sb = split(b.bytes(), 16);
        List<byte[]> frames = new ArrayList<>(sa.frames());
        frames.set(1, sb.frames().get(1));
        assertEquals(2 * CHUNK, decrypt(sa.withFrames(frames).join(), dek).length);
    }

    @Test
    void wholeHeaderSwappedForAnotherFiles_isFileIdMismatch() {
        byte[] dek = newDek();
        Split victim = v2Split(dek, randomBytes(CHUNK + 1, 16));
        Split donor = v2Split(dek, randomBytes(CHUNK + 1, 17));
        assertEquals(Reason.FILE_ID_MISMATCH, failureReason(victim.withHeader(donor.header()).join(), dek));
    }

    @Test
    void strippedV2Header_downgradeAttempt_isVersionMismatch() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(CHUNK + 1, 18));
        // Forge a v1-looking header: just the edek_id, so the key still resolves.
        byte[] v1Header = Arrays.copyOfRange(s.header(), 5, 21);
        assertEquals(Reason.VERSION_MISMATCH, failureReason(s.withHeader(v1Header).join(), dek));
    }

    @Test
    void headerChunkSizeChanged_isHeaderMismatch() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(3 * CHUNK, 19));
        byte[] header = s.header().clone();
        ByteBuffer.wrap(header, 37, 4).putInt(CHUNK * 2);
        assertEquals(Reason.HEADER_MISMATCH, failureReason(s.withHeader(header).join(), dek));
    }

    @Test
    void trailingBytesAfterFinalChunk_areRejected() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(CHUNK + 1, 20));
        List<byte[]> frames = new ArrayList<>(s.frames());
        frames.add(s.frames().get(0));
        assertEquals(Reason.TRAILING_DATA, failureReason(s.withFrames(frames).join(), dek));
    }

    @Test
    void flippedCiphertextBit_isAuthFailed_bothVersions() {
        byte[] dek = newDek();
        for (Version v : Version.values()) {
            Encrypted enc = encrypt(randomBytes(CHUNK + 1, 21), EDEK, dek, new EncryptedFileWriter.Options(v, CHUNK, false));
            byte[] tampered = enc.bytes().clone();
            tampered[tampered.length - 3] ^= 0x01;
            assertEquals(Reason.AUTH_FAILED, failureReason(tampered, dek), v.name());
        }
    }

    @Test
    void wrongKey_isAuthFailed() {
        Split s = v2Split(newDek(), randomBytes(10, 22));
        assertEquals(Reason.AUTH_FAILED, failureReason(s.join(), newDek()));
    }

    // ---- limits (a long-running service must not be crashable by one crafted file) ----

    @Test
    void oversizedFrameLength_isRejectedBeforeAllocating() {
        byte[] dek = newDek();
        Split s = v2Split(dek, randomBytes(10, 23));
        byte[] huge = new byte[]{0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}; // ~2 GiB claimed
        byte[] tampered = concat(s.header(), huge);
        assertEquals(Reason.LIMIT_EXCEEDED, failureReason(tampered, dek));
    }

    @Test
    void v1_frameLimit_comesFromLimitsOnly() {
        byte[] dek = newDek();
        Encrypted enc = encrypt(randomBytes(CHUNK, 24), EDEK, dek, EncryptedFileWriter.Options.v1(CHUNK, false));
        EncryptedFileReader.Limits tight = new EncryptedFileReader.Limits(1024, 1024);
        EncryptedFileException e = assertThrows(EncryptedFileException.class, () -> decrypt(enc.bytes(), dek, tight));
        assertEquals(Reason.LIMIT_EXCEEDED, e.reason());
    }

    @Test
    void headerChunkSizeAboveLimit_isRejectedAtOpen() {
        byte[] dek = newDek();
        Encrypted enc = encrypt(randomBytes(10, 25), EDEK, dek, EncryptedFileWriter.Options.v2(8 * 1024 * 1024, false));
        EncryptedFileReader.Limits tight = new EncryptedFileReader.Limits(16 * 1024 * 1024, 1024 * 1024);
        EncryptedFileException e = assertThrows(EncryptedFileException.class,
                () -> EncryptedFileReader.open(new ByteArrayInputStream(enc.bytes()), tight));
        assertEquals(Reason.LIMIT_EXCEEDED, e.reason());
    }

    @Test
    void gzipBomb_isBoundedByChunkSize() throws Exception {
        // A writer holding the key could still craft a chunk that inflates far past
        // chunk_size; decompression must stop at the cap rather than exhaust memory.
        byte[] dek = newDek();
        UUID fileId = EncryptedFileFormat.newFileId();
        FileHeader header = new FileHeader(Version.V2, EDEK, fileId, CHUNK);
        byte[] bomb = ChunkPayload.encode(header, 0, true, new byte[64 * CHUNK], true);
        DekManager.EncryptResult enc = DekManager.encrypt(bomb, dek, OWNER);
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        java.io.DataOutputStream out = new java.io.DataOutputStream(file);
        EncryptedFileFormat.writeHeader(out, header);
        out.writeInt(28 + enc.ciphertext().length);
        out.write(enc.iv());
        out.write(enc.tag());
        out.write(enc.ciphertext());
        assertEquals(Reason.LIMIT_EXCEEDED, failureReason(file.toByteArray(), dek));
    }

    // ---- rescue path: per-chunk decrypt through a core-shaped token ----

    @Test
    void rescueViaCoreToken_appliesTheSameChecks() {
        byte[] dek = newDek();
        byte[] plaintext = randomBytes(3 * CHUNK + 3, 26);
        Encrypted enc = encrypt(plaintext, EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, true));

        // Stand-in for hsm-core-service's /decrypt: unpack the token, decrypt with the DEK.
        EncryptedFileReader.ChunkDecryptor core = (header, frame) -> {
            String token = EncryptedFileReader.toCoreServiceToken(header, frame);
            DekManager.UnpackedToken t = DekManager.unpackToken(token);
            assertEquals(EDEK, t.edekId());
            return DekManager.decrypt(t.ciphertext(), t.tag(), t.iv(), dek, OWNER);
        };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileReader.ReadResult r = EncryptedFileReader.open(new ByteArrayInputStream(enc.bytes())).decryptTo(out, core);
        assertArrayEquals(plaintext, out.toByteArray());
        assertEquals(4, r.chunkCount());

        Split s = split(enc.bytes(), EncryptedFileFormat.V2_HEADER_BYTES);
        byte[] truncated = s.withFrames(s.frames().subList(0, 3)).join();
        EncryptedFileException e = assertThrows(EncryptedFileException.class,
                () -> EncryptedFileReader.open(new ByteArrayInputStream(truncated)).decryptTo(new ByteArrayOutputStream(), core));
        assertEquals(Reason.TRUNCATED, e.reason());
    }

    @Test
    void chunkPayloadDecode_acceptsCoreResponseString() {
        byte[] dek = newDek();
        Encrypted enc = encrypt(randomBytes(20, 27), EDEK, dek, EncryptedFileWriter.Options.v2(CHUNK, false));
        EncryptedFileReader.Session session = EncryptedFileReader.open(new ByteArrayInputStream(enc.bytes()));
        EncryptedFileReader.Frame frame = session.nextFrame();
        assertNotNull(frame);
        byte[] pt;
        try {
            pt = DekManager.decrypt(frame.ciphertext(), frame.tag(), frame.iv(), dek, OWNER);
        } catch (javax.crypto.AEADBadTagException ex) {
            throw new AssertionError(ex);
        }
        String asCoreReturnsIt = new String(pt, StandardCharsets.UTF_8);
        ChunkPayload.Decoded d = ChunkPayload.decode(session.header(), 0, asCoreReturnsIt, CHUNK);
        assertTrue(d.isFinal());
        assertEquals(20, d.payload().length);
    }
}

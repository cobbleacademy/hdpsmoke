// Reference implementation, in C#/.NET, of reading a REAL hsm-bulk-client
// FileBulkJob-produced file -- format v1 or v2 -- and decrypting it via
// hsm-core-service's own POST /decrypt/batch directly. This is also the
// "rescue" path: any encrypted file can be recovered through core alone.
//
// Normative spec: java/docs/FILE_FORMAT.md. Reference implementation:
// hsm-crypto-client's EncryptedFileFormat / ChunkPayload / EncryptedFileReader.
// If this class and the Java code disagree, the Java code and the golden files
// in hsm-crypto-client/src/test/resources/golden/ win.
//
//     v1 header (16 B):  edek_id
//     v2 header (41 B):  "HSMF" | 0x02 | edek_id(16) | file_id(16) | chunk_size(int32 BE)
//     frames (both):     repeat { length(int32 BE) | iv(12) | tag(16) | ciphertext }
//
// Token for core's /decrypt (DekManager.packToken):
//     "v1." + base64url(0x01 + edek_id(16) + iv(12) + tag(16) + ciphertext)
//
// Decrypted chunk plaintext (base64 text of):
//     v1: marker(0x00 raw | 0x01 gzip) | payload
//     v2: marker(0x02 raw | 0x03 gzip) | file_id(16) | chunk_index(int64 BE)
//         | is_final(0x00|0x01) | chunk_size(int32 BE) | payload
//
// For v2 this class enforces the same rules as the Java reader (file_id,
// position, chunk size, exactly one final chunk, nothing after it) and rejects
// a v2 chunk inside a v1-looking file (header stripped: downgrade attempt).
// edek_id and file_id stay raw byte[16] throughout -- never System.Guid, so
// there's no Java-UUID-vs-.NET-Guid byte-order pitfall.
//
// Reuses HsmCoreClient from HsmCoreBatchFile.cs for the /decrypt/batch call.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Hsm.BulkClient.Examples
{
    public sealed class FileIntegrityException : InvalidDataException
    {
        public FileIntegrityException(string message) : base(message) { }
    }

    public static class HsmBulkFileReader
    {
        private const int IvLength = 12;
        private const int TagLength = 16;
        private const int FrameOverhead = IvLength + TagLength;
        private const int V1HeaderBytes = 16;
        private const int V2HeaderBytes = 41;
        private const int V2BindingBytes = 16 + 8 + 1 + 4;
        private static readonly byte[] V2Magic = { (byte)'H', (byte)'S', (byte)'M', (byte)'F', 0x02 };
        private static readonly byte[] TokenVersion = { 0x01 };
        private const string TokenPrefix = "v1.";

        // Same defaults as hsm-file-service: hard caps whatever a header claims.
        public static int MaxFrameBytes { get; set; } = 16 * 1024 * 1024;
        public static int MaxChunkBytes { get; set; } = 12 * 1024 * 1024;

        public sealed record FileHeader(int Version, byte[] EdekId, byte[]? FileId, int ChunkSize);

        private sealed record Frame(byte[] Iv, byte[] Tag, byte[] Ciphertext);

        public static FileHeader ReadHeader(byte[] data)
        {
            if (data.Length >= V2Magic.Length && data.AsSpan(0, V2Magic.Length).SequenceEqual(V2Magic))
            {
                if (data.Length < V2HeaderBytes) throw new FileIntegrityException("truncated v2 header");
                int chunkSize = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(37, 4));
                if (chunkSize <= 0 || chunkSize > MaxChunkBytes)
                    throw new FileIntegrityException($"v2 header chunk_size {chunkSize} out of range");
                return new FileHeader(2, data[5..21], data[21..37], chunkSize);
            }
            if (data.Length < V1HeaderBytes) throw new FileIntegrityException("too short to contain a 16-byte edek_id header");
            return new FileHeader(1, data[..16], null, 0);
        }

        private static List<Frame> ReadFrames(byte[] data, FileHeader header)
        {
            int pos = header.Version == 2 ? V2HeaderBytes : V1HeaderBytes;
            var frames = new List<Frame>();
            while (pos < data.Length)
            {
                if (pos + 4 > data.Length) throw new FileIntegrityException($"truncated frame-length field at frame {frames.Count}");
                int frameLen = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(pos, 4)); // matches DataInputStream.readInt
                pos += 4;
                if (frameLen <= FrameOverhead || frameLen > MaxFrameBytes)
                    throw new FileIntegrityException($"frame {frames.Count} has invalid length {frameLen}");
                if (pos + frameLen > data.Length) throw new FileIntegrityException($"truncated frame body at frame {frames.Count}");
                frames.Add(new Frame(data[pos..(pos + IvLength)], data[(pos + IvLength)..(pos + FrameOverhead)],
                    data[(pos + FrameOverhead)..(pos + frameLen)]));
                pos += frameLen;
            }
            return frames;
        }

        /// <summary>
        /// Ports EncryptedFileReader.toCoreServiceToken() exactly. URL-safe base64 WITH
        /// padding -- matches Java's Base64.getUrlEncoder() default.
        /// </summary>
        public static string ReconstructCoreServiceToken(byte[] edekId, byte[] iv, byte[] tag, byte[] ciphertext)
        {
            byte[] payload = new byte[1 + 16 + iv.Length + tag.Length + ciphertext.Length];
            int offset = 0;
            Buffer.BlockCopy(TokenVersion, 0, payload, offset, 1); offset += 1;
            Buffer.BlockCopy(edekId, 0, payload, offset, 16); offset += 16;
            Buffer.BlockCopy(iv, 0, payload, offset, iv.Length); offset += iv.Length;
            Buffer.BlockCopy(tag, 0, payload, offset, tag.Length); offset += tag.Length;
            Buffer.BlockCopy(ciphertext, 0, payload, offset, ciphertext.Length);
            return TokenPrefix + Convert.ToBase64String(payload).Replace('+', '-').Replace('/', '_');
        }

        /// <summary>Decodes one decrypted chunk and applies the per-chunk rules for the header's version.</summary>
        public static (byte[] Payload, bool IsFinal) DecodeChunk(FileHeader header, long index, string base64Plaintext)
        {
            byte[] marked;
            try { marked = Convert.FromBase64String(base64Plaintext); }
            catch (FormatException) { throw new FileIntegrityException($"chunk {index} is not valid base64"); }
            if (marked.Length == 0) throw new FileIntegrityException($"chunk {index} is empty");

            byte marker = marked[0];
            bool v2Marker = marker == 0x02 || marker == 0x03;
            bool v1Marker = marker == 0x00 || marker == 0x01;
            if (!v1Marker && !v2Marker) throw new FileIntegrityException($"chunk {index} has unknown marker 0x{marker:x2}");
            if (header.Version == 1 && v2Marker)
                throw new FileIntegrityException($"chunk {index} is a v2 chunk in a v1-looking file (header stripped / downgrade)");
            if (header.Version == 2 && v1Marker) throw new FileIntegrityException($"chunk {index} is a v1 chunk in a v2 file");
            bool gz = marker == 0x01 || marker == 0x03;

            if (header.Version == 1)
            {
                byte[] body1 = marked[1..];
                return (gz ? Gunzip(body1, MaxChunkBytes, index) : body1, false);
            }

            if (marked.Length < 1 + V2BindingBytes) throw new FileIntegrityException($"chunk {index} too short for v2 binding fields");
            byte[] fileId = marked[1..17];
            long chunkIndex = BinaryPrimitives.ReadInt64BigEndian(marked.AsSpan(17, 8));
            byte finalFlag = marked[25];
            int chunkSize = BinaryPrimitives.ReadInt32BigEndian(marked.AsSpan(26, 4));
            if (!fileId.AsSpan().SequenceEqual(header.FileId)) throw new FileIntegrityException($"chunk {index} belongs to a different file (spliced)");
            if (chunkIndex != index) throw new FileIntegrityException($"chunk at position {index} carries index {chunkIndex} (reordered/duplicated/dropped)");
            if (finalFlag > 1) throw new FileIntegrityException($"chunk {index} has invalid is_final flag");
            if (chunkSize != header.ChunkSize) throw new FileIntegrityException($"chunk {index} chunk_size {chunkSize} != header {header.ChunkSize}");

            byte[] body = marked[(1 + V2BindingBytes)..];
            byte[] payload = gz ? Gunzip(body, header.ChunkSize, index) : body;
            if (payload.Length > header.ChunkSize) throw new FileIntegrityException($"chunk {index} payload exceeds chunk_size");
            bool isFinal = finalFlag == 1;
            if (!isFinal && payload.Length != header.ChunkSize)
                throw new FileIntegrityException($"non-final chunk {index} holds {payload.Length} bytes, expected {header.ChunkSize}");
            return (payload, isFinal);
        }

        /// <summary>
        /// Decrypts a whole file held in memory. decryptTokens maps core tokens to their
        /// decrypted base64 plaintexts, in order -- normally core's /decrypt/batch.
        /// </summary>
        public static async Task<byte[]> DecryptBytesAsync(byte[] data, Func<List<string>, Task<List<string>>> decryptTokens)
        {
            FileHeader header = ReadHeader(data);
            List<Frame> frames = ReadFrames(data, header);
            var tokens = frames.Select(f => ReconstructCoreServiceToken(header.EdekId, f.Iv, f.Tag, f.Ciphertext)).ToList();
            List<string> plaintexts = tokens.Count == 0 ? new List<string>() : await decryptTokens(tokens);

            using var output = new MemoryStream();
            bool finalSeen = false;
            for (int i = 0; i < plaintexts.Count; i++)
            {
                if (finalSeen) throw new FileIntegrityException($"data follows the final chunk (chunk {i})");
                (byte[] payload, bool isFinal) = DecodeChunk(header, i, plaintexts[i]);
                output.Write(payload, 0, payload.Length);
                finalSeen = isFinal;
            }
            if (header.Version == 2 && !finalSeen)
                throw new FileIntegrityException($"file ended after {frames.Count} chunk(s) without a final chunk (truncated)");
            return output.ToArray();
        }

        /// <summary>
        /// Reads a FileBulkJob-produced file (v1 or v2) and decrypts it purely via
        /// hsm-core-service's /decrypt/batch. The target is written only after every
        /// check has passed (temp file + move), so a failed rescue never leaves a
        /// plausible-looking partial file behind.
        /// </summary>
        public static async Task DecryptBulkFileAsync(HsmCoreClient client, string sourcePath, string targetPath)
        {
            byte[] data = await File.ReadAllBytesAsync(sourcePath);
            byte[] plaintext = await DecryptBytesAsync(data, async tokens =>
            {
                var items = new List<Dictionary<string, object?>>(tokens.Count);
                for (int i = 0; i < tokens.Count; i++)
                    items.Add(new Dictionary<string, object?> { ["key"] = i.ToString(), ["ciphertext"] = tokens[i] });
                Dictionary<string, JsonElement> results = await client.DecryptItemsAsync(items);
                var ordered = new List<string>(tokens.Count);
                for (int i = 0; i < tokens.Count; i++)
                    ordered.Add(results[i.ToString()].GetProperty("plaintext").GetString()!);
                return ordered;
            });
            string partial = targetPath + ".partial";
            await File.WriteAllBytesAsync(partial, plaintext);
            File.Move(partial, targetPath, overwrite: true);
        }

        /// <summary>Bounded decompression: stops at limit bytes so a crafted chunk can't exhaust memory.</summary>
        private static byte[] Gunzip(byte[] data, int limit, long index)
        {
            try
            {
                using var input = new MemoryStream(data);
                using var gzip = new GZipStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream();
                byte[] buffer = new byte[64 * 1024];
                int n;
                while ((n = gzip.Read(buffer, 0, buffer.Length)) > 0)
                {
                    if (output.Length + n > limit) throw new FileIntegrityException($"chunk {index} decompresses beyond limit {limit}");
                    output.Write(buffer, 0, n);
                }
                return output.ToArray();
            }
            catch (InvalidDataException e) when (e is not FileIntegrityException)
            {
                throw new FileIntegrityException($"chunk {index} has invalid gzip data");
            }
        }
    }
}

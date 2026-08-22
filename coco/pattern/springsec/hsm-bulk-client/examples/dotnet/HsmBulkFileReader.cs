// Reference implementation, in C#/.NET, of reading a REAL hsm-bulk-client
// FileBulkJob-produced file (Tier 3 -- the file hsm-bulk-service's local
// encrypt path writes) and decrypting it via hsm-core-service's own
// POST /decrypt directly. Zero contact with hsm-bulk-service on this side.
//
// Complements HsmCoreBatchFile.cs (the Tier 1 pattern: chunk and encrypt
// directly against hsm-core-service, with its own JSON manifest). This
// class instead reads a file that already went through hsm-bulk-service's
// Tier 3 pipeline, and decrypts it purely via hsm-core-service, with no
// adapter beyond parsing the file's own binary layout -- proving the two
// services' ciphertext is genuinely, mutually interoperable, not just
// similar.
//
// FileBulkJob.java's class javadoc and reconstructCoreServiceToken() are
// the canonical source of truth this class ports.
//
// File layout (FileBulkJob.java):
//
//     [8 bytes: edek_id most-significant bits, big-endian signed long]
//     [8 bytes: edek_id least-significant bits, big-endian signed long]
//     repeated until EOF:
//         [4 bytes: frame length N, big-endian signed int]
//         [12 bytes: AES-GCM IV/nonce]
//         [16 bytes: AES-GCM authentication tag]
//         [N - 28 bytes: ciphertext]
//
// hsm-core-service's ciphertext token format (DekManager.packToken):
//
//     "v1." + base64url(0x01 [version byte] + edek_id(16, big-endian) +
//                        iv(12) + tag(16) + ciphertext)
//
// Deliberately never constructs a System.Guid anywhere in this class --
// edek_id is carried as raw byte[16] throughout, exactly the file's own
// layout. This directory's earlier, now-superseded Tier 3 reference needed
// the Guid<->UUID byte-order conversion because its own API surface took/
// returned Guid; this class has no such need, since all it ever does with
// edek_id is concatenate its bytes into a reconstructed token -- so
// there's no Java-UUID-vs-.NET-Guid pitfall here at all.
//
// Reuses HsmCoreClient from HsmCoreBatchFile.cs (same project, same
// namespace) for the actual /decrypt/batch call, rather than duplicating
// an HTTP client.
//
// The first byte after base64-decoding a chunk's decrypted plaintext is a
// compressed/raw marker -- 0x01 means FileBulkJob gzipped it before
// encryption (compress-before-encrypt: true on that job, see
// ClientProperties.File's javadoc), 0x00 means it didn't. Always checked
// here regardless of any config of this class's own -- every chunk is
// self-describing, so there's nothing to configure to match whatever job
// produced the file.

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text.Json;
using System.Threading.Tasks;

namespace Hsm.BulkClient.Examples
{
    public static class HsmBulkFileReader
    {
        private const int IvLength = 12;
        private const int TagLength = 16;
        private static readonly byte[] TokenVersion = { 0x01 };
        private const string TokenPrefix = "v1.";

        private sealed record Frame(byte[] Iv, byte[] Tag, byte[] Ciphertext);

        private static int ReadFully(Stream stream, byte[] buffer)
        {
            int total = 0;
            while (total < buffer.Length)
            {
                int n = stream.Read(buffer, total, buffer.Length - total);
                if (n <= 0) break;
                total += n;
            }
            return total;
        }

        private static (byte[] EdekId, List<Frame> Frames) ReadEdekIdAndFrames(string sourcePath)
        {
            using FileStream source = File.OpenRead(sourcePath);
            byte[] edekId = new byte[16];
            if (ReadFully(source, edekId) != 16)
                throw new InvalidDataException($"{sourcePath}: too short to contain a 16-byte edek_id header");

            var frames = new List<Frame>();
            byte[] lenBuf = new byte[4];
            while (true)
            {
                int lenRead = ReadFully(source, lenBuf);
                if (lenRead == 0) break; // clean end of frames
                if (lenRead != 4)
                    throw new InvalidDataException($"{sourcePath}: truncated frame-length field near end of file");
                byte[] lenBufBE = (byte[])lenBuf.Clone();
                if (BitConverter.IsLittleEndian) Array.Reverse(lenBufBE); // matches DataInputStream.readInt
                int frameLen = BitConverter.ToInt32(lenBufBE, 0);

                byte[] frame = new byte[frameLen];
                if (ReadFully(source, frame) != frameLen)
                    throw new InvalidDataException($"{sourcePath}: truncated frame body near end of file");

                byte[] iv = frame[..IvLength];
                byte[] tag = frame[IvLength..(IvLength + TagLength)];
                byte[] ciphertext = frame[(IvLength + TagLength)..];
                frames.Add(new Frame(iv, tag, ciphertext));
            }
            return (edekId, frames);
        }

        /// <summary>
        /// Ports FileBulkJob.reconstructCoreServiceToken() exactly: rebuilds
        /// the same "v1.&lt;base64url(...)&gt;" string hsm-core-service's own
        /// /encrypt produces, from one frame's raw bytes plus the file's
        /// 16-byte edek_id. URL-safe base64 WITH padding -- matches Java's
        /// Base64.getUrlEncoder() default (no .withoutPadding()) exactly.
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

            string base64Url = Convert.ToBase64String(payload).Replace('+', '-').Replace('/', '_');
            return TokenPrefix + base64Url;
        }

        /// <summary>
        /// Reads a real FileBulkJob-produced file and decrypts it purely via
        /// hsm-core-service's /decrypt/batch -- zero contact with
        /// hsm-bulk-service on this side. Chunks are already in file order
        /// (frames appear in the exact order they were written), so no
        /// separate ordering key is needed the way HsmCoreBatchFile's own
        /// manifest needs one -- the file itself is already an ordered
        /// record.
        /// </summary>
        public static async Task DecryptBulkFileAsync(HsmCoreClient client, string sourcePath, string targetPath)
        {
            (byte[] edekId, List<Frame> frames) = ReadEdekIdAndFrames(sourcePath);

            var items = new List<Dictionary<string, object?>>(frames.Count);
            for (int i = 0; i < frames.Count; i++)
            {
                string token = ReconstructCoreServiceToken(edekId, frames[i].Iv, frames[i].Tag, frames[i].Ciphertext);
                items.Add(new Dictionary<string, object?> { ["key"] = i.ToString(), ["ciphertext"] = token });
            }

            Dictionary<string, JsonElement> results = await client.DecryptItemsAsync(items);

            using FileStream outStream = File.Create(targetPath);
            for (int i = 0; i < frames.Count; i++)
            {
                string base64Plaintext = results[i.ToString()].GetProperty("plaintext").GetString()!;
                byte[] marked = Convert.FromBase64String(base64Plaintext);
                byte flag = marked[0];
                byte[] payload = marked[1..];
                byte[] chunk = flag == 0x01 ? Gunzip(payload) : payload;
                outStream.Write(chunk, 0, chunk.Length);
            }
        }

        private static byte[] Gunzip(byte[] data)
        {
            using var input = new MemoryStream(data);
            using var gzip = new GZipStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            gzip.CopyTo(output);
            return output.ToArray();
        }
    }
}

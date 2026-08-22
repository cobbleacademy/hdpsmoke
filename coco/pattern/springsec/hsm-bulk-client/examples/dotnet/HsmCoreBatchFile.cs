// Reference implementation, in C#/.NET, of the Tier 1 (reviewed,
// foundational) pattern for encrypting/decrypting large files: chunk the
// file yourself, but send each chunk's actual data straight to
// hsm-core-service's own POST /encrypt/batch and POST /decrypt/batch --
// hsm-core-service does the real AES-256-GCM work server-side and hands
// back one opaque ciphertext token per chunk. No hsm-bulk-service anywhere
// in this picture, and no raw DEK ever reaches this code -- this file
// never touches System.Security.Cryptography at all. See
// java/docs/BULK_OPERATIONS.md's "Files with multiple chunks: chunking +
// stitch-back" section, which this follows directly: chunking strategy,
// chunk identity caveat, manifest, and stitch-back verification are all
// straight from that section, not invented here.
//
// (There is a second, different pattern -- this directory's own
// now-superseded HsmFileFormat.cs implemented it -- where a separate
// hsm-bulk-service hands the client a wrapped DEK for local AES-GCM.
// That's a later, "proposed, not yet approved"/PoC-stage addition (Tier
// 3), not the reviewed foundational one. This file is Tier 1:
// hsm-core-service directly, nothing else running.)
//
// Wire contract, straight from EncryptController/DecryptController's
// actual DTOs (com.hsm.core.dto) -- every JSON field below is snake_case,
// since hsm-core-service's whole API uses
// spring.jackson.property-naming-strategy: SNAKE_CASE:
//
//     POST {baseUrl}{apiV1Prefix}/encrypt/batch
//     Authorization: Bearer <token>
//     X-App-ID: <appId>
//     X-Response-Detail: minimal   (default; this module never needs the
//                                   full-view-only fields)
//     {"items": [{"key": "<correlation id>", "plaintext": "<base64>",
//                 "encoding": "base64", ...}]}
//     -> {"items": [{"key": "...", "status": "success"|"error",
//                     "result": {"ciphertext": "v1....", ...} | null,
//                     "detail": null | "<error message>"}]}
//
//     POST {baseUrl}{apiV1Prefix}/decrypt/batch -- same shape, ciphertext
//     tokens in, {"plaintext": "...", "encoding": "..."} results out.
//
// Plaintext is ALWAYS sent/received as base64 here, never raw UTF-8 text --
// a file chunk is arbitrary binary, not necessarily valid text.
//
// Batch size cap is hsm.service.batch-max-items on the server (default
// 100, shared by encrypt and decrypt) -- this class batches into groups of
// at most that many chunks per HTTP call.
//
// Auth: getting a bearer token/app_id is entirely outside this class's
// scope -- a real deployment uses an Entra ID/Azure AD app registration
// doing OAuth2 client-credentials against hsm-core-service's own
// JWT_AUDIENCE/JWT_ISSUER; a local demo-mode server instead accepts one of
// a handful of fixed literal strings like "demo-token-payments-svc" (not a
// template for real auth). See java/docs/APP_ONBOARDING.md.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Hsm.BulkClient.Examples
{
    public class HsmCoreBatchException : Exception
    {
        public HsmCoreBatchException(string message) : base(message) { }
    }

    public sealed class BatchItemResult
    {
        [JsonPropertyName("key")] public string Key { get; set; } = "";
        [JsonPropertyName("status")] public string Status { get; set; } = "";
        [JsonPropertyName("result")] public JsonElement? Result { get; set; }
        [JsonPropertyName("detail")] public string? Detail { get; set; }
    }

    public sealed class BatchItemsEnvelope
    {
        [JsonPropertyName("items")] public List<BatchItemResult> Items { get; set; } = new();
    }

    /// <summary>
    /// Thin wrapper around the two batch endpoints this module needs -- not
    /// a general hsm-core-service client, just enough to drive file
    /// encrypt/decrypt via chunking. baseUrl/apiV1Prefix/appId/token are
    /// exactly what SvcClient.java's Java equivalent takes for
    /// hsm-bulk-service, applied here to hsm-core-service instead.
    /// </summary>
    public sealed class HsmCoreClient
    {
        public const int DefaultBatchMaxItems = 100;

        private readonly string _baseUrl;
        private readonly string _apiV1Prefix;
        private readonly string _appId;
        private readonly string _token;
        private readonly int _batchMaxItems;
        private readonly HttpClient _http;

        public HsmCoreClient(string baseUrl, string apiV1Prefix, string appId, string token,
            int batchMaxItems = DefaultBatchMaxItems, HttpClient? httpClient = null)
        {
            _baseUrl = baseUrl;
            _apiV1Prefix = apiV1Prefix;
            _appId = appId;
            _token = token;
            _batchMaxItems = batchMaxItems;
            _http = httpClient ?? new HttpClient();
        }

        private async Task<List<BatchItemResult>> PostBatchAsync(string path, List<Dictionary<string, object?>> items)
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}{_apiV1Prefix}{path}");
            req.Headers.Add("Authorization", $"Bearer {_token}");
            req.Headers.Add("X-App-ID", _appId);
            req.Headers.Add("X-Response-Detail", "minimal"); // this module only ever needs ciphertext/plaintext + encoding
            req.Content = JsonContent.Create(new Dictionary<string, object?> { ["items"] = items });

            using HttpResponseMessage resp = await _http.SendAsync(req);
            resp.EnsureSuccessStatusCode(); // a 4xx/5xx here means the WHOLE batch was rejected (bad auth, over-cap, empty)
            var envelope = await resp.Content.ReadFromJsonAsync<BatchItemsEnvelope>();
            return envelope!.Items;
        }

        public async Task<Dictionary<string, JsonElement>> EncryptItemsAsync(List<Dictionary<string, object?>> items)
        {
            var byKey = new Dictionary<string, JsonElement>();
            for (int i = 0; i < items.Count; i += _batchMaxItems)
            {
                var batch = items.GetRange(i, Math.Min(_batchMaxItems, items.Count - i));
                foreach (BatchItemResult item in await PostBatchAsync("/encrypt/batch", batch))
                {
                    if (item.Status != "success")
                        throw new HsmCoreBatchException($"encrypt failed for key={item.Key}: {item.Detail}");
                    byKey[item.Key] = item.Result!.Value;
                }
            }
            return byKey;
        }

        public async Task<Dictionary<string, JsonElement>> DecryptItemsAsync(List<Dictionary<string, object?>> items)
        {
            var byKey = new Dictionary<string, JsonElement>();
            for (int i = 0; i < items.Count; i += _batchMaxItems)
            {
                var batch = items.GetRange(i, Math.Min(_batchMaxItems, items.Count - i));
                foreach (BatchItemResult item in await PostBatchAsync("/decrypt/batch", batch))
                {
                    if (item.Status != "success")
                        throw new HsmCoreBatchException($"decrypt failed for key={item.Key}: {item.Detail}");
                    byKey[item.Key] = item.Result!.Value;
                }
            }
            return byKey;
        }
    }

    public sealed class FileManifest
    {
        [JsonPropertyName("file_id")] public string FileId { get; set; } = "";
        [JsonPropertyName("filename")] public string Filename { get; set; } = "";
        [JsonPropertyName("total_size_bytes")] public long TotalSizeBytes { get; set; }
        [JsonPropertyName("chunk_size_bytes")] public int ChunkSizeBytes { get; set; }
        [JsonPropertyName("chunk_count")] public int ChunkCount { get; set; }
        [JsonPropertyName("plaintext_sha256")] public string PlaintextSha256 { get; set; } = "";
        [JsonPropertyName("chunks")] public List<string> Chunks { get; set; } = new();
    }

    public static class HsmCoreBatchFile
    {
        public const int DefaultChunkSizeBytes = 8 * 1024 * 1024;

        /// <summary>
        /// Chunks sourcePath locally, encrypts each chunk via /encrypt/batch,
        /// and writes a manifest (JSON: file_id, filename, sizes, ordered
        /// ciphertext tokens, whole-file plaintext SHA-256) to manifestPath --
        /// your own record, in your own storage, per BULK_OPERATIONS.md's
        /// guidance; nothing here is created or stored by hsm-core-service
        /// itself.
        /// </summary>
        public static async Task<FileManifest> EncryptFileAsync(HsmCoreClient client, string sourcePath, string manifestPath,
            int chunkSizeBytes = DefaultChunkSizeBytes, string? dataClassification = null, string? dekName = null)
        {
            using var sha256 = SHA256.Create();
            var items = new List<Dictionary<string, object?>>();

            using (FileStream source = File.OpenRead(sourcePath))
            {
                byte[] buffer = new byte[chunkSizeBytes];
                int index = 0;
                int read;
                while ((read = ReadFully(source, buffer)) > 0)
                {
                    byte[] chunk = read == buffer.Length ? buffer : buffer[..read];
                    sha256.TransformBlock(chunk, 0, chunk.Length, null, 0);

                    var item = new Dictionary<string, object?>
                    {
                        ["key"] = index.ToString(),
                        ["plaintext"] = Convert.ToBase64String(chunk),
                        ["encoding"] = "base64",
                    };
                    if (dataClassification != null) item["data_classification"] = dataClassification;
                    if (dekName != null) item["dek_name"] = dekName;
                    items.Add(item);
                    index++;
                }
            }
            sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            string plaintextSha256 = Convert.ToHexString(sha256.Hash!).ToLowerInvariant();

            Dictionary<string, JsonElement> results = await client.EncryptItemsAsync(items);
            // Re-order by numeric key rather than trust response array order --
            // the server never documents item-order preservation as a
            // contract, only that each item's own "key" is echoed back
            // correctly.
            var orderedCiphertexts = new List<string>(items.Count);
            for (int i = 0; i < items.Count; i++)
                orderedCiphertexts.Add(results[i.ToString()].GetProperty("ciphertext").GetString()!);

            var manifest = new FileManifest
            {
                FileId = Path.GetFileName(sourcePath),
                Filename = Path.GetFileName(sourcePath),
                TotalSizeBytes = new FileInfo(sourcePath).Length,
                ChunkSizeBytes = chunkSizeBytes,
                ChunkCount = items.Count,
                PlaintextSha256 = plaintextSha256,
                Chunks = orderedCiphertexts,
            };
            File.WriteAllText(manifestPath, JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true }));
            return manifest;
        }

        /// <summary>
        /// Reads a manifest written by EncryptFileAsync, decrypts every chunk
        /// via /decrypt/batch, reassembles them in manifest order, and
        /// verifies the reassembled plaintext's SHA-256 against what
        /// EncryptFileAsync recorded -- the check that catches a chunk
        /// silently dropped or duplicated during reassembly (the per-chunk
        /// AEAD tag alone only proves each chunk's ciphertext wasn't
        /// tampered with, not that this method assembled them correctly).
        /// Written to a temp file first, renamed into place only after the
        /// digest check passes -- targetPath is never left holding a
        /// partially-wrong file.
        /// </summary>
        public static async Task DecryptFileAsync(HsmCoreClient client, string manifestPath, string targetPath)
        {
            var manifest = JsonSerializer.Deserialize<FileManifest>(File.ReadAllText(manifestPath))!;
            var items = new List<Dictionary<string, object?>>(manifest.ChunkCount);
            for (int i = 0; i < manifest.Chunks.Count; i++)
                items.Add(new Dictionary<string, object?> { ["key"] = i.ToString(), ["ciphertext"] = manifest.Chunks[i] });

            Dictionary<string, JsonElement> results = await client.DecryptItemsAsync(items);

            string tmpPath = targetPath + ".tmp";
            using (var sha256 = SHA256.Create())
            using (FileStream outStream = File.Create(tmpPath))
            {
                for (int i = 0; i < manifest.ChunkCount; i++)
                {
                    JsonElement result = results[i.ToString()];
                    string plaintextField = result.GetProperty("plaintext").GetString()!;
                    string encoding = result.GetProperty("encoding").GetString()!;
                    byte[] chunk = encoding == "base64" ? Convert.FromBase64String(plaintextField) : Encoding.UTF8.GetBytes(plaintextField);
                    sha256.TransformBlock(chunk, 0, chunk.Length, null, 0);
                    outStream.Write(chunk, 0, chunk.Length);
                }
                sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                string actualSha256 = Convert.ToHexString(sha256.Hash!).ToLowerInvariant();

                if (actualSha256 != manifest.PlaintextSha256)
                {
                    outStream.Close();
                    File.Delete(tmpPath);
                    throw new HsmCoreBatchException(
                        $"{manifestPath}: reassembled plaintext SHA-256 ({actualSha256}) does not match " +
                        $"manifest ({manifest.PlaintextSha256}) -- a chunk was dropped, duplicated, or reordered during reassembly");
                }
            }
            File.Move(tmpPath, targetPath, overwrite: true);
        }

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
    }
}

// Package objstore defines the object storage seams for disaggregated mode.
//
// Storage is split into two layers (implementation plan D2):
//
//   - Blob is the raw key transport: put, get, ranged get, and delete of
//     opaque bytes under string keys. It trusts nothing and verifies nothing.
//     Implementations are S3 (objstore/s3) and in-memory (objstore/memblob).
//     Fault injection lives at this layer.
//   - The object protocol over a Blob and the catalog (design §7) lives in
//     objstore/protocol. Objects are addressed by catalog object_id.
//     protocol.Uploader does dedup, the uploading row, and read-back verify
//     on upload. protocol.Reader, which implements Store, verifies on read
//     and refreshes and retries once. Bytes returned by a Blob are untrusted
//     until Verify accepts them. The protocol is a subpackage so that this
//     package stays free of catalog types and anything can import it.
//   - objstore/objcache is the compressed object cache (design §17), an LRU
//     keyed by SHA-256 that the Reader consults before the Blob.
//
// Objects are immutable. Every upload attempt uses a fresh random key, so a
// key is never overwritten and the Blob layer needs no conditional writes or
// versioning. Keys never contain user data.
package objstore

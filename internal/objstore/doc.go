// Package objstore defines the object storage seams for disaggregated mode.
//
// Storage is split into two layers (implementation plan D2):
//
//   - Blob is the raw key transport: put, get, ranged get, and delete of
//     opaque bytes under string keys. It trusts nothing and verifies nothing.
//     Implementations are S3 (objstore/s3) and in-memory (objstore/memblob).
//     Fault injection lives at this layer.
//   - Store is the object protocol over a Blob and the catalog (design §7):
//     objects addressed by catalog object_id, with dedup, the uploading row,
//     read-back verify on Put, and verify-on-read for Get. Bytes returned by a
//     Blob are untrusted until Verify accepts them.
//
// Objects are immutable. Every upload attempt uses a fresh random key, so a
// key is never overwritten and the Blob layer needs no conditional writes or
// versioning. Keys never contain user data.
package objstore

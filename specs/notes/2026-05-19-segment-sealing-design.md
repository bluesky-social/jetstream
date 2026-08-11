# Segment File Sealing — Footer, Finalized Header, and Reader

This historical design specified the sealed segment format: finalized header, block and collection indexes, segment/per-block DID blooms, checksum, and a concurrent reader. It defined seal ordering, crash recovery, idempotency, corruption detection, and the reader API while preserving the active append format. The authoritative implementation area referenced was the public `segment` package, especially header, footer, bloom, collection, seal, and reader code.

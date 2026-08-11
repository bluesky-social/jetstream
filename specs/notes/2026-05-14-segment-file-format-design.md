# Segment File Format — Initial Slice

This historical design defined the initial append-only segment writer and compressed block framing, with explicit concurrency, validation, recovery, and testing goals. It deliberately deferred sealing, indexes, blooms, and the reader to later slices. The principal code references were the public `segment` package and its writer, block, fuzz, swarm, golden, and benchmark tests.

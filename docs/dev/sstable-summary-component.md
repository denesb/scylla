```c
struct summary_entry {
    char key[...]; // variable-length.
    be64 position;
};

struct summary_la {
    struct header {
        be32 min_index_interval;
        be32 size;
        be64 memory_size;
        be32 sampling_level;
        be32 size_at_full_sampling;
    } header;

    le32 positions[header.size];
    summary_entry entries[header.size];

    be32 first_key_size;
    char first_key[first_key_size];

    be32 last_key_size;
    char last_key[last_key_size];
};
```

Note that while most integers stored in the SSTable files are encoded in big-endian order (a.k.a. "network order", least significant byte last), the items in the `positions` array are exceptional in that they are stored in little-endian order (`le32` above). Actually, Cassandra stores them in the machine's *native* order, which might be different on different machine (making sstables not interoperable between them!). In Scylla we write the positions in little-endian order to allow interoperability with 1. summary files written by Cassandra on the more common little endian machines, and 2. summary files written by Scylla on the more rare big-endian machines.

Each `summary_entry` holds a key, which can have a variable length, but does not describe it own length (e.g., it is not preceded by a length, nor is it a null-terminated string). Instead, the `positions` array is used to say where each `entry` is relative to the end of the header (whose size is fixed). For example, if in some case `size` is 1, then `positions` is an array of 1 item, with the value 4, because the first `summary_entry` starts 4 bytes after the header (right after the 4-byte single position).

To know where the last `summary_entry` ends, we need one more position, and this, somewhat confusingly, is stored not as an additional entry in the `positions` array, but rather as `memory_size` in the header. This "memory_size" is the offset of the end of the entries array, again relative to the end of the header.

## Segment boundaries

After the above data, Cassandra's summary file includes information about **segment boundaries**. Cassandra typically uses mmap to read files (see Config.DiskAccessMode). But Java cannot mmap more than 2 GB in one mapping (see http://nyeggen.com/post/2014-05-18-memory-mapping-%3E2gb-of-data-in-java/), so it maps the file in "segments" and uses SegmentedFile to stitch those segments together. Apparently (though I'm not quite sure about the details) Cassandra makes an effort to pick the segment boundaries so a single partition isn't split between different segments.

Each of the index and data files can be segmented in this way, and the summary file ends in a description of the segments of both these files, first of the index file and then of the data file. The code that writes this information is at the end of SSTableReader.saveSummary():

```java
    private void saveSummary(SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder, IndexSummary summary)
            // Write the data explained above:
            ...
            ByteBufferUtil.writeWithLength(first.getKey(), oStream);
            ByteBufferUtil.writeWithLength(last.getKey(), oStream);
            // This is what writes segment boundaries we are discussing now.
            ibuilder.serializeBounds(oStream);
            dbuilder.serializeBounds(oStream);
```

The base class SegmentedFile.Builder.serializeBounds() write the disk access mode, which is "mmap" (written as the two bytes \00 \04 and then mmap), and then MmappedSegmentedFile.Builder.serializeBounds writes the number of segments (32 bit) and the position of each of these segments.

When the files have size < 2 GB, we should always see at the end of the summary file the bytes "\0 \4 m m a p \0 \0 \0 \1 \0 \0 \0 \0 \0 \0 \0 \0" repeated twice (once for the index file, once for the data file).


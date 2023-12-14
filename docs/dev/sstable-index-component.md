The sstable _index file_, together with the [[SSTables Summary File]], provide a way to efficiently locate an sstable row in the [[SSTables Data File]] with a specific key.

Beyond looking for a specific key, the index file also allows (in a way we'll explain below) to coarsely seek inside very long rows and find specific columns - without needing to read the entire row from the data file.

Broadly, the _index file_ lists the keys in the data file in order, giving for each key its position in the data file. The _summary file_, which is held in memory, contains a sample of these keys, pointing to their position in the index file. So to efficiently search for a specific key, we use the summary to find a (relatively short) range of positions in the index file which may hold this key - and then read this range from the index file, and look for the specific key. The details on how to further pinpoint specific columns inside very long rows are explained below.

# The index file
The index file is a long sequence of entries:

```c
struct index_file {
    struct index_entry entries[]; 
}

struct index_entry {
    be16 key_length;
    char key[key_length];
    be64 position;
    be32 promoted_index_length;
    char promoted_index[promoted_index_length];
}
```

The `key` is the sstable row's key, and `position` is the position in the data file of that row.

For an extremely long row (in CQL nomenclature, a partition with many rows), when we need to find a specific range of columns, just finding the beginning of the row is not efficient enough. So the "promoted index" was added, which samples the row at roughly `column_index_size_in_kb` (by default, 64 KB) blocks and gives the column name range for each block.

The reason for the strange name "promoted index" is that it originally started as a separately stored column index, until in Cassandra 1.2, [CASSANDRA-2319](https://issues.apache.org/jira/browse/CASSANDRA-2319), it was "promoted" to live inside the sstable index file, reducing the number of seeks required to find a given column from 3 to 2.

The structure of the promoted index (when promoted_index_length != 0) is as follows:

```c
struct promoted_index {
   struct deletion_time deletion_time;
   be32 num_blocks;
   index_info infos[num_blocks];
}

struct index_info {
   be16 start_name_length;
   char start_name[start_name_length];
   be16 end_name_length;
   char end_name[end_name_length];
   be64 offset;
   be64 width;
}
```

A promoted index for a row divides the row into blocks of roughly 64 KB in size, each summarized by an `index_info` structure: `start_name` and `end_name` are the names of the first and last columns in that block, respectively. `offset` is the start of the block relative to the beginning of the row in the data file, and `width` is the length of the block (there is no guarantee that all blocks have exactly the same length).

The number of blocks, `num_blocks`, is always bigger than 1: For one block, the promoted index is pointless and would not be written in the first place. 

Interestingly, the `promoted_index_length` field is not necessary for deserializing the promoted_index - the deserializer just checks if it is 0 (and no promoted_index follows) or non-zero (and a promoted_index follows). Nevertheless, Cassandra does write a correct promoted_index_length, which is the size in bytes of the to-be-serialized promoted_index, and it uses this length field when it needs to efficiently skip an index_entry.

The `deletion_time` structure is explained in [[SSTables Data File]] about sstable rows. It is simply a copy of the row's `deletion_time` - either LIVE or a row tombstone. We need this copy inside the index, because normally the `deletion_time` of an sstable row appears in its very beginning of the row (in the data file), but with the promoted index we intend to skip directly to the middle of a very large row, so we don't want to need to also read the beginning of the row just to read its row tombstone.

## Implementation in Cassandra
In Cassandra (version 2), `RowIndexEntry` holds one index_entry. This base class comes with no promoted index (i.e, promoted_index_length==0), and the ambiguously-named subclass `RowIndexEntry.IndexedEntry` is one which additionally holds a promoted index (i.e., a sample of columns in that row). Code in the `RowIndexEntry` class serializes and deserializes this promoted index.

The promoted index, also known as a "columns index", is a `List<IndexHelper.IndexInfo>` (i.e. `IndexHelper.IndexInfo` holds one `index_info`).

The method `DatabaseDescriptor.getColumnIndexSize()` returns the configured block size in bytes (taken from the `column_index_size_in_kb` configuration option, defaulting to 64 KB).

The code building the promoted index is `SSTableWriter` which uses the class `ColumnIndex` to actually build the promoted index. It iterates through the content of the row, cells and range tombstones, ending a block and moving to the next block if the block is over the desired size (`DatabaseDescriptor.getColumnIndexSize()`). The desired block size is only an approximate goal, and there is no harm if we go over it - in particular the code has a special-case when the entire block is filled with range tombstones, Cassandra lets it grow even more so it does contains cells.

The code which _uses_ the promoted index during reads is in `IndexedSliceReader`. This code knows it needs to read just a slice (a range) of columns, and can use the index to skip parts of a very long sstable row, instead of reading it all. This code is fairly complex, as it also handles various complications like multiple slices and iterating in reverse.

The code which actually reads the index entry from disk (and expands the serialized promoted index into a list of `IndexInfo` objects) is in `SSTableReader`. Cassandra has a *key cache* (see [this blog entry](http://www.datastax.com/dev/blog/maximizing-cache-benefit-with-cassandra)) which holds some of the previously read `RowIndexEntry` objects - and this includes the entire promoted index of that key, already parsed in IndexInfo objects. This key caching makes sense if the key cache can be bigger than the row cache, or for paging through a large partition. Caching the already-parsed objects also makes it possible to do a binary search on the promoted index.

## More about tombstones
The fact that we have a `deletion_time` field in the promoted index makes it unnecessary to write a copy of this tombstone to every block of the partition. However, **range tombstones** may still apply to more than one block: For example a range tombstone may cover a large range of cql rows which end up written to multiple indexed blocks. Because the promoted index adds the ability to read only one of these blocks without reading the ones before it, we need to write in the beginning of each block all the range tombstones which still apply at the beginning of this block.

In the Cassandra code, this logic happens in `ColumnIndex.add(OnDiskAtom column)`:

1. It uses a `RangeTombstone.Tracker` object to process the cells and range-tombstone beginnings it sees in name order, and track which range tombstones are still "open" at every point: A range tombstone is still "open" if our walk hasn't progressed beyond its end.

2. If we are starting a new block, the function tombstoneTracker.writeOpenedMarker() is called. This function knows from the tombstone tracker which tombstones are still "open" at this point, and writes those at the beginning of the new block. The writeOpenedMarker() also includes code to avoid writing a tombstone if it is "superseded" by another (supersedes means more recent and covers a bigger range).

3. Note that the original range tombstone - not just its intersection with the block - is written in each block. This means that a reader which reads the entire partition (not just one block) no longer sees the invariant that range tombstones and cells are ordered in increasing column (beginning column, for range tombstone) order - some range tombstone may a second time out-of-order. Hopefully this doesn't break anything, but we need to consider this issue carefully.

## Notes
For truely huge sstable rows that we want to read tiny parts of, even the promoted index is not efficient enough, because the promoted index (and therefore the row index entry) can grow very large, and we need to read it and deserialize it all. Cassandra 3, with its different sstable format, introduced a different technique for skipping in very long rows - see [CASSANDRA-11206](https://issues.apache.org/jira/browse/CASSANDRA-11206). 

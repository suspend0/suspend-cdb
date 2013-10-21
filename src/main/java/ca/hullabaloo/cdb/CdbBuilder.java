package ca.hullabaloo.cdb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Implementation of the write side of a CDB file.
 *
 * @see Cdb
 * @see CdbFile
 */
class CdbBuilder implements Cdb.Builder {
  public static final int HEADER_SIZE = 2048;
  private final ByteBuffer lengths = ByteBuffer.allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN);

  // SeekableByteChannel, wherefore art thou?
  private final File file;
  private final FileChannel target;
  private final TableBuilder[] tables = new TableBuilder[256];

  private boolean closed = false;

  CdbBuilder(File target) throws IOException {
    for (int i = 0; i < this.tables.length; i++) {
      this.tables[i] = new TableBuilder();
    }

    this.file = target;
    this.target = new RandomAccessFile(target, "rw").getChannel();
    this.target.truncate(0);
    this.target.position(HEADER_SIZE);
  }

  @Override public void put(ByteBuffer key, ByteBuffer value) throws IOException {
    if (closed) throw new IllegalStateException();
    int hash = CdbHash.hash(key);
    long pos = this.target.position();
    try {
      this.lengths.putInt(key.remaining()).putInt(value.remaining()).flip();
      this.target.write(this.lengths);
      this.target.write(key);
      this.target.write(value);

      TableBuilder t = tables[CdbHash.table(hash)];
      t.add(hash, pos);

    } finally {
      this.lengths.clear();
    }
  }

  @Override public File build() throws IOException {
    close();
    return this.file;
  }

  @Override public void close() throws IOException {
    if (!closed) {
      try {
        writeTables();
      } finally {
        this.closed = true;
        this.target.close();
      }
    }
  }

  private static int checkPosition(long l) throws IOException {
    int i = (int) l;
    if (i != l) throw new IOException("CDB Files must be less than 4GB");
    return i;
  }

  /**
   * There are two tables: a header table {@link CdbHash#table} that points to
   * hash tables at the end of the file.  This writes them both.
   */
  private void writeTables() throws IOException {
    IntBuffer header = this.target.map(FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    for (TableBuilder t : tables) {
      if (t.size() == 0) {
        header.put(0).put(0);
      } else {
        header.put(checkPosition(this.target.position())).put(t.size());
        this.target.write(t.toBuffer());
      }
    }
    // checkCast here to make sure file size is < 4GB
    this.target.truncate(checkPosition(this.target.position()));
  }

  /**
   * Builds a hash table mapping <b>hash</b> to <b>position</b> in the file,
   * packed into one array (alternating slots)
   */
  private static class TableBuilder {
    private int[] hashAndPositionPairs = new int[256];
    private int idx;

    public void add(int hash, long filePosition) throws IOException {
      ensureCapacity();
      hashAndPositionPairs[idx++] = hash;
      hashAndPositionPairs[idx++] = checkPosition(filePosition);
    }

    private void ensureCapacity() {
      if (idx == hashAndPositionPairs.length) {
        hashAndPositionPairs = Arrays.copyOf(hashAndPositionPairs, idx * 2);
      }
    }

    public int size() {
      // table size is 2 * number of elements
      // which is coincidentally this value
      return idx;
    }

    public ByteBuffer toBuffer() {
      int tableSize = size();

      ByteBuffer bb = ByteBuffer.allocate(2 * tableSize * (Integer.SIZE / Byte.SIZE))
          .order(ByteOrder.LITTLE_ENDIAN);
      IntBuffer table = bb.asIntBuffer();
      assert table.capacity() == 2 * tableSize : "Two integers per slot";

      for (int i = 0; i < idx - 1; i += 2) {
        int hash = hashAndPositionPairs[i];
        int pos = hashAndPositionPairs[i + 1];
        int slot = CdbHash.slot(hash, tableSize);
        while (table.get(slot * 2) != 0) {
          slot += 1;
          slot %= tableSize;
        }
        table.put(slot * 2, hash);
        table.put(slot * 2 + 1, pos);
      }
      //we don't need to flip the buffer b/c we only use absolute setters
      return bb;
    }
  }
}

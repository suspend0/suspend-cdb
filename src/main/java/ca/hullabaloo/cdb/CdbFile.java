package ca.hullabaloo.cdb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * The implementation of the read-side of a CDB file.
 *
 * @see Cdb
 * @see CdbBuilder
 */
class CdbFile {
  private static final int INTEGER_BYTES = Integer.SIZE / Byte.SIZE;
  private static final int SLOT_BYTES = INTEGER_BYTES * 2;

  class RecordIterator {
    private final ByteBuffer view;
    private int keyLength;
    private int valueLength;

    RecordIterator() {
      ByteBuffer bb = duplicate();
      bb.position(header.length * INTEGER_BYTES);
      bb.limit(tablesPointer);
      this.view = bb.slice().order(bb.order());
    }

    public boolean canAdvance() {
      return view.remaining() - keyLength - valueLength > 0;
    }

    public void advance() {
      Buffers.advance(view, keyLength + valueLength);
      keyLength = view.getInt();
      valueLength = view.getInt();
    }

    public ByteBuffer key() {
      return Buffers.slice(view, view.position(), keyLength);
    }

    public ByteBuffer value() {
      return Buffers.slice(view, view.position() + keyLength, valueLength);
    }
  }

  // the entire file mapped into memory
  private final MappedByteBuffer data;

  // A cache of the header's 2k
  private final int[] header;

  // Where the hash tables start
  private final int tablesPointer;

  CdbFile(File file) throws IOException {
    FileChannel source = new RandomAccessFile(file, "r").getChannel();
    this.data = source.map(FileChannel.MapMode.READ_ONLY, 0, source.size());
    this.data.order(ByteOrder.LITTLE_ENDIAN);
    this.header = new int[512];
    this.data.asIntBuffer().get(this.header);
    // Search the header for the first hash table
    int firstTable = this.data.capacity();
    for (int i = 0; i < header.length; i += 2) {
      int pos = header[i];
      if (pos > 0) { firstTable = Math.min(firstTable, pos); }
    }
    this.tablesPointer = firstTable;
  }

  public ByteBuffer get(ByteBuffer key) {
    // read the header to find the position of this key's hash table
    int hash = CdbHash.hash(key);
    int index = CdbHash.table(hash) * 2;
    int tablePos = header[index];
    if (tablePos == 0) return null;
    int slotCount = header[index + 1];
    return find(tablePos, slotCount, key, hash);
  }

  /**
   * Linear probe of a hash table searching for a key
   *
   * @param tablePos The position of the hash table in the file
   * @param slotCount the number of slots in the table.  Each slot is two integers (8 bytes), and
   *                  contains the hash value and the position in the file of the the data
   * @param key the key to find
   * @param hash the key's hash value
   *
   * @return The value's data, as a view into the file, or null if no key is found
   */
  private ByteBuffer find(int tablePos, int slotCount, ByteBuffer key, int hash) {
    int slot = CdbHash.slot(hash, slotCount);
    int probes = 0;
    do {
      int possibleHash = getHashAtSlot(tablePos, slot);
      if (hash == possibleHash) {
        // This is view over both key and value
        ByteBuffer record = getRecordAtSlot(tablePos, slot);
        int keyLen = record.getInt();
        int valueLen = record.getInt();
        // view over the key
        record.limit(record.position() + keyLen);
        if (record.equals(key)) {
          // reposition over value
          record.position(record.limit()).limit(record.limit() + valueLen);
          return record;
        }
      } else if (0 == possibleHash) {
        return null;
      }
      slot = (slot + 1) % slotCount;
    } while (++probes < slotCount);
    return null;
  }

  /**
   * The number of records stored in the CDB
   */
  public int size() {
    // count the number of used slots in all tables;
    int count = 0;
    for (int i = this.tablesPointer; i < this.data.capacity(); i += SLOT_BYTES) {
      count += -this.data.getInt(i) >>> (Integer.SIZE - 1);
    }
    return count;
  }

  RecordIterator records() {
    return new RecordIterator();
  }

  private int getHashAtSlot(int tablePos, int slot) {
    return this.data.getInt(tablePos + (slot * SLOT_BYTES));
  }

  private ByteBuffer getRecordAtSlot(int tablePos, int slot) {
    int pos = this.data.getInt(tablePos + (slot * SLOT_BYTES) + INTEGER_BYTES);
    ByteBuffer record = duplicate();
    record.position(pos);
    return record;
  }

  private ByteBuffer duplicate() {
    ByteBuffer bb = this.data.duplicate();
    bb.order(this.data.order());
    return bb;
  }
}

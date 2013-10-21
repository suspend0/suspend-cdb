package ca.hullabaloo.cdb;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A map view of a cdb
 */
class CdbMap extends AbstractMap<ByteBuffer, ByteBuffer> {
  private final CdbFile file;
  private Set<Entry<ByteBuffer, ByteBuffer>> entrySet;
  private int size = -1;

  CdbMap(CdbFile cdbFile) {
    file = cdbFile;
  }

  @Override public int size() {
    int sz = size;
    return sz != -1 ? sz : (size = file.size());
  }

  @Override public ByteBuffer get(Object key) {
    return key instanceof ByteBuffer ? file.get((ByteBuffer) key) : null;
  }

  @Override public boolean containsKey(Object key) {
    return get(key) != null;
  }

  private Iterator<Entry<ByteBuffer, ByteBuffer>> newEntryIterator() {
    return new Iter(file.records());
  }

  // Not really sure how to fix "Missing NotNull"
  @SuppressWarnings("NullableProblems")
  @Override public Set<Entry<ByteBuffer, ByteBuffer>> entrySet() {
    Set<Entry<ByteBuffer, ByteBuffer>> es = this.entrySet;
    return (es != null) ? es : (entrySet = new EntrySet());
  }

  private final class EntrySet extends AbstractSet<Entry<ByteBuffer, ByteBuffer>> {
    // Not really sure how to fix "Missing NotNull"
    @Override @SuppressWarnings("NullableProblems")
    public Iterator<Entry<ByteBuffer, ByteBuffer>> iterator() {
      return newEntryIterator();
    }

    @Override public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) { return false; }
      // We're careful to only have buffers in our map
      @SuppressWarnings("unchecked")
      Map.Entry<ByteBuffer, ByteBuffer> e = (Map.Entry<ByteBuffer, ByteBuffer>) o;
      ByteBuffer candidate = CdbMap.this.get(e.getKey());
      return candidate != null && candidate.equals(e.getValue());
    }

    @Override public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override public int size() {
      return CdbMap.this.size();
    }

    @Override public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private class Iter implements Iterator<Entry<ByteBuffer, ByteBuffer>> {
    private final CdbFile.RecordIterator records;

    public Iter(CdbFile.RecordIterator records) {
      this.records = records;
    }

    @Override public boolean hasNext() {
      return records.canAdvance();
    }

    @Override public Entry<ByteBuffer, ByteBuffer> next() {
      if(!hasNext()) throw new NoSuchElementException();

      records.advance();
      return new SimpleImmutableEntry<ByteBuffer, ByteBuffer>(
          records.key(), records.value());
    }

    @Override public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

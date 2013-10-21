package ca.hullabaloo.cdb;

import java.nio.ByteBuffer;

class CdbHash {
  static final long INT_MASK = 0xffffffffL;

  public static int hash(ByteBuffer key) {
    int h = 5381;
    for (int p = key.position(), N = key.limit(); p < N; p++) {
      int c = key.get(p);
      h = ((h << 5) + h) ^ c;
    }
    return h;
  }

  public static int table(int hash) {
    // hash % 256 according to spec, but we always want a positive value
    return hash & 255;
  }

  public static int slot(int hash, int length) {
    // hash / 265 using unsigned math
    int s = (int) ((hash & INT_MASK) / 256L);
    return s % length;
  }
}

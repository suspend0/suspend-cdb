package ca.hullabaloo.cdb;

import java.nio.Buffer;
import java.nio.ByteBuffer;

class Buffers {
  static <B extends Buffer> B advance(B buffer, int amount) {
    buffer.position(buffer.position() + amount);
    return buffer;
  }

  static ByteBuffer slice(ByteBuffer buffer, int pos, int len) {
    int l = buffer.limit();
    int p = buffer.position();
    try {
      buffer.position(pos).limit(pos + len);
      return buffer.slice().order(buffer.order());
    } finally {
      buffer.position(p);
      buffer.limit(l);
    }
  }
}

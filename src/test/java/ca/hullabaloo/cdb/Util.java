package ca.hullabaloo.cdb;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Util {
  static ByteBuffer bytes(String s) {
    return ByteBuffer.wrap(s.getBytes(Charset.forName("ASCII")));
  }
}

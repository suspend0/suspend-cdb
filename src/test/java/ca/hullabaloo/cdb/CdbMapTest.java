package ca.hullabaloo.cdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static ca.hullabaloo.cdb.Util.bytes;

public class CdbMapTest {
  @ClassRule public static TemporaryFolder tmp = new TemporaryFolder();

  private Map<ByteBuffer, ByteBuffer> cdb;

  @Before public void setup() throws IOException {
    Cdb.Builder b = Cdb.builder(tmp.newFile());
    b.put(bytes("susan"), bytes("victoria"));
    b.put(bytes("ted"), bytes("tucker"));
    cdb = Cdb.open(b.build());
  }

  @Test public void contains() {
    Assert.assertTrue(cdb.containsKey(bytes("susan")));
  }

  @SuppressWarnings("SuspiciousMethodCalls")
  @Test public void containsStringThrowsNoException() {
    Assert.assertFalse(cdb.containsKey("susan"));
    Assert.assertFalse(cdb.containsKey(null));
  }

  @Test public void iterator() {
    List<Map.Entry<ByteBuffer, ByteBuffer>> expected = new ArrayList<Map.Entry<ByteBuffer, ByteBuffer>>();
    expected.add(new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(bytes("susan"), bytes("victoria")));
    expected.add(new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(bytes("ted"), bytes("tucker")));

    List<Map.Entry<ByteBuffer, ByteBuffer>> actual = new ArrayList<Map.Entry<ByteBuffer, ByteBuffer>>();
    for (Map.Entry<ByteBuffer, ByteBuffer> byteBufferByteBufferEntry : cdb.entrySet()) {
      actual.add(byteBufferByteBufferEntry);
    }

    Assert.assertEquals(expected, actual);
  }
}

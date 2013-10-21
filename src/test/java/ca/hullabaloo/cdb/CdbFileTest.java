package ca.hullabaloo.cdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ReadOnlyBufferException;

import static ca.hullabaloo.cdb.Util.bytes;

public class CdbFileTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private CdbFile cdb;

  @Before public void setUp() throws IOException {
    CdbBuilder b = new CdbBuilder(tmp.newFile());
    b.put(bytes("Hi"), bytes("There"));
    cdb = new CdbFile(b.build());
  }

  @Test public void readOne() throws IOException {
    Assert.assertEquals(bytes("There"), cdb.get(bytes("Hi")));
  }

  @Test public void size() throws Exception {
    Assert.assertEquals(1, cdb.size());
  }

  @Test public void readNull() throws IOException {
    Assert.assertNull(cdb.get(bytes("foo")));
  }

  @Test(expected = ReadOnlyBufferException.class)
  public void readOnly() throws IOException {
    cdb.get(bytes("Hi")).putInt(17);
  }
}

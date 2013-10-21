package ca.hullabaloo.cdb;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static ca.hullabaloo.cdb.Util.bytes;

public class CdbBuilderTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  @Test public void oneValue() throws IOException {
    CdbBuilder b = new CdbBuilder(tmp.newFile());
    b.put(bytes("Hi"), bytes("there"));
    File f = b.build();
    int len = 2048; // header
    len += 2 + 5; // data
    len += (Integer.SIZE / Byte.SIZE) * 2; // two length integers
    len += (Integer.SIZE / Byte.SIZE) * 2 * 2; // one two-slot hash table
    Assert.assertEquals(len, f.length());
  }

  @Test public void manyValues() throws IOException {
    CdbBuilder b = new CdbBuilder(tmp.newFile());
    b.put(bytes("Hi"), bytes("there"));
    b.put(bytes("how"), bytes("are"));
    b.put(bytes("you"), bytes("doing"));
    b.put(bytes("today"), bytes("in"));
    b.put(bytes("the"), bytes("rain"));
    File f = b.build();
    int len = 2048; // header
    len += "Hitherehowareyoudoingtodayintherain".length(); // data
    len += 5 * 24;
    Assert.assertEquals(len, f.length());
  }
}

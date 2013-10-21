package ca.hullabaloo.cdb;

import org.junit.Assert;
import org.junit.Test;

import static ca.hullabaloo.cdb.Util.bytes;

public class CdbHashTest {
  @Test public void wellKnown() {
    // From http://b0llix.net/ezcdb/site.cgi?page=cdb.5
    Assert.assertEquals(CdbHash.hash(bytes("ABJ")), 0x0b87b6ac);
    Assert.assertEquals(CdbHash.hash(bytes("ABK")), 0x0b87b6ad);
    Assert.assertEquals(CdbHash.hash(bytes("ABL")), 0x0b87b6aa);
    Assert.assertEquals(CdbHash.hash(bytes("ABM")), 0x0b87b6ab);
  }

}

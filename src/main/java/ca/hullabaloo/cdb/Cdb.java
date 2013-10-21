package ca.hullabaloo.cdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Classes in this package create and access files in the "constant database" format.  Constant DBs are
 * immutable hash-based structures thought up by D. J. Bernstein.
 * <p/>
 * As an additional limitation, this package supports files up to @{link Integer#MAX_VALUE} in size.
 * Regular cdb has a 4GB maximum.
 *
 * @see <a href="http://cr.yp.to/cdb/cdb.txt">http://cr.yp.to/cdb/cdb.txt</a>
 * @see <a href="http://www.unixuser.org/~euske/doc/cdbinternals/">http://www.unixuser.org/~euske/doc/cdbinternals/</a>
 */
public class Cdb {
  /**
   * Returns a builder that will write a <code>cdb</code> to the provided path.
   * Any existing file is truncated.
   * <p/>
   * Overhead is 24 bytes per entry, plus a 2K header.
   */
  public static Builder builder(File file) throws IOException {
    return new CdbBuilder(file);
  }

  /**
   * Returns a fast, immutable map over the provided <code>cdb</code> file.  The map is
   * thread-safe.
   * <p/>
   * The file is mapped into memory, and the buffers returned by this map are all "zero-copy".
   * Care should be taken to not hold onto the buffers after the map has been collected.
   */
  public static Map<ByteBuffer, ByteBuffer> open(File file) throws IOException {
    return new CdbMap(new CdbFile(file));
  }

  public interface Builder extends Closeable {
    public void put(ByteBuffer key, ByteBuffer value) throws IOException;

    public File build() throws IOException;
  }
}

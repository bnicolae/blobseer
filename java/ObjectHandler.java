package blobseer;


import java.io.IOException;
/*
import java.nio.ByteBuffer;
import java.util.Properties;
import java.io.ByteArrayInputStream;
*/

public class ObjectHandler
{

    // the pointer the C++ object_handler
    private long blob;

    private final static native long blob_init(String CfgFile);
    private final static native boolean blob_free(long Blob);
    
    private final static native bool blob_create(long blob, long ps, int rc);
    private final static native bool get_latest(long blob, int id);

    private final static native bool read(long blob, long offset, long size, byte[] buffer, int version);
    private final static native bool append(long blob, long size, byte[] buffer);
    private final static native bool write(long blob, long offset, long size, byte[] buffer);

    private final static native int get_objcount(long blob);
    private final static native long get_size(long blob, int version);
    private final static native int get_version(long blob);
    private final static native int get_page_size(long blob);
    private final static native int get_id(long blob);

    static {
        try {
            System.loadLibrary("blobseer-java");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load blobseer-java native library");
            System.exit(1);
        }
    }

    // The constructor basically creates a new C++ object_handler with the specified config file
    public ObjectHandler(String ConfigFile) throws IOException
    {
        blob = blob_init(ConfigFile);
        if (blob == 0) {
            throw new IOException("Unable to initialize a new blob with config file: " + ConfigFile);
        }
    }

    // Delete the blob when the Java GC decides to destroy this object
    protected void finalize() throws Throwable {
	try {
	    blob_free(blob);
	} finally {
	    super.finalize();
	}
    }
}

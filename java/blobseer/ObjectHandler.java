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
    
    private final static native boolean blob_create(long blob, long ps, int rc);
    private final static native boolean get_latest(long blob, int id);

    private final static native boolean read(long blob, long offset, long size, byte[] buffer, int version);
    private final static native boolean append(long blob, long size, byte[] buffer);
    private final static native boolean write(long blob, long offset, long size, byte[] buffer);

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
    
    // Create a new blob
    public boolean blobCreate(long ps, int rc) {
		return blob_create(blob, ps, rc);
    }
    
    // Get latest version of the blob
    public boolean getLatest(int id) {
	    return get_latest(blob, id);
    }
	
    // Read the blob
    public boolean read(long offset, long size, byte[] buffer, int version) {
	    return read(blob, offset, size, buffer, version);
    }
	public boolean read(long offset, long size, byte[] buffer) {
		int version = this.getVersion();
	    return read(blob, offset, size, buffer, version);
    }
    
    // Append memory
    public boolean append(long size, byte[] buffer) {
	    return append(blob, size, buffer);
    }

    public boolean append(long size) {
	    byte[] buffer = new byte[(int)size];
		for(int i=0;i<size;i++) buffer[i] = (byte)0;
	    return append(blob, size, buffer);
    }
    
    // Write into the blob
    public boolean write(long offset, long size, byte[] buffer) {
	    return write(blob, offset, size, buffer);
    }

    // Get the number of blobs
    public int getObjCount() {
	    return get_objcount(blob);
    }
    
    // Get the size of the blob
    public long getSize(int version) {
	    return get_size(blob, version);
    }
     public long getSize() {
		int version = this.getVersion();
	    return get_size(blob, version);
    }
    
    // Get the version
    public int getVersion() {
	    return get_version(blob);
    }
    
    // Get the page size
    public int getPageSize() {
	    return get_page_size(blob);
    }
    
    // Get the current id
    public int getId() {
	    return get_id(blob);
    }
}

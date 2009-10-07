import blobseer.ObjectHandler;
import java.io.IOException;
import java.util.Vector;
import java.nio.ByteBuffer;

class Test {	
    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) {	    
	if(args.length != 1) {
	    System.out.println("Usage : java Test <configfile>");
	    return;
	}

	ObjectHandler handler;
	ByteBuffer buffer = ByteBuffer.allocateDirect(2 * BUFFER_SIZE);
	for (int i = 0; i < 2 * BUFFER_SIZE; i++)
	    buffer.put(i, (byte)0);
				
	try {
	    handler = new ObjectHandler(args[0]);
	} catch(IOException e){
	    System.err.println("IOException catched.");
	    return;
	}
		
	// trying to create a blob
	handler.blobCreate(BUFFER_SIZE, 1);
		
	// trying to append 2 pages of memory
	
	handler.append(2 * BUFFER_SIZE, buffer);
		
	// trying to write something
	for(int i=0; i<BUFFER_SIZE; i++) 
	    buffer.put(i, (byte)i);
	handler.write(0, BUFFER_SIZE, buffer);
		
	// trying to re-read
	ByteBuffer result = ByteBuffer.allocateDirect(BUFFER_SIZE);
	for(int i = 0; i < BUFFER_SIZE; i++) 
	    result.put(i, (byte)0);
	handler.getLatest(handler.getId());
	handler.read(0, BUFFER_SIZE, result,0);
		
	// compare the result with what we have written
	boolean c = true;
	for(int i = 0; i < BUFFER_SIZE; i++) 
	    c = c & (result.get(i) == (byte)(i % 256));
	System.out.println(c);
		
	// other tests
	System.out.println(handler.getObjCount());
	System.out.println(handler.getSize());
	System.out.println(handler.getPageSize());
	System.out.println(handler.getId());

	Vector v = new Vector();
	handler.getLocations(0, 2048, v);
	System.out.println(v);
    }
}

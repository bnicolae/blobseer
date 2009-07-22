import blobseer.ObjectHandler;
import java.io.IOException;
import java.util.Vector;

/* ========================== 
   Test program for the java binding
   created by Matthieu Dorier
   date : 5.17.2009
   =========================== */
class Test {
	
    public static void main(String[] args) {	    
	ObjectHandler handler;
	byte[] buffer = new byte[1024];
		
	if(args.length != 1)
	    {
		System.out.println("Usage : java Test <configfile>");
		return;
	    }
		
	try {
	    handler = new ObjectHandler(args[0]);
	}catch(IOException e){
	    System.err.println("IOException catched.");
	    return;
	}
		
	// trying to create a blob
	handler.blobCreate(1024, 1);
		
	// trying to append 2 pages of memory
	handler.append(2048);
		
	// trying to write something
	for(int i=0;i<1024;i++) buffer[i] = (byte)i;
	handler.write(0, 1024, buffer);
		
	// trying to re-read
	byte[] result = new byte[1024];
	for(int i=0;i<1024;i++) result[i] = (byte)0;
	handler.getLatest(handler.getId());
	handler.read(0,1024,result,0);
		
	// compare the result with what we have written
	boolean c = true;
	for(int i=0;i<1024;i++) c = c & (result[i] == (byte)(i%256));
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

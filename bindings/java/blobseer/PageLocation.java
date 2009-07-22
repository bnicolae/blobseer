package blobseer;

public class PageLocation {
    public String host, port;
    public long offset, size;

    public PageLocation(String h, String p, long o, long s) {
	host = h;
	port = p;
	offset = o;
	size = s;
    }

    public String toString() {
	return "(" + host + ":" + port + ", " + offset + ", " + size + ")";
    }
}

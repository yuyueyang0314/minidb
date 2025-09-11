package com.minidb.storage;
import java.nio.ByteBuffer;
public class Page {
    public final int pageId;
    public final ByteBuffer buf;
    public Page(int pageId, byte[] data){
        this.pageId = pageId;
        this.buf = ByteBuffer.wrap(data);
    }
}

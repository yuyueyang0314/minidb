package com.minidb.storage;
import java.nio.ByteBuffer;
import java.util.*;
import com.minidb.catalog.*;
import com.minidb.utils.*;
public class TableHeap {
    private final int tableId;
    private final Schema schema;
    private final FileManager fm;
    private final BufferPool bp;
    // Page layout: [header: int nRecords][slotOffsets...][free space][records]
    public TableHeap(int tableId, Schema schema, FileManager fm, BufferPool bp){
        this.tableId = tableId; this.schema = schema; this.fm = fm; this.bp = bp;
    }
    private Page loadPage(int pid){
        Page p = bp.get(tableId, pid);
        if (p==null){ p = fm.readPage(tableId, pid); bp.put(tableId, pid, p); }
        return p;
    }
    private int numPages(){
        long sz = fm.fileSize(tableId);
        return (int)(sz / Constants.PAGE_SIZE);
    }
    private ByteBuffer buf(Page p){ return p.buf; }
    private boolean tryInsertInto(Page p, Record r){
        ByteBuffer b = buf(p);
        b.position(0);
        int n = b.getInt();
        if (n < 0 || n > 10000){ // uninitialized page
            b.clear();
            b.putInt(0); n = 0;
        }
        int header = 4 + (n+1)*4;
        int freePtr = (n==0) ? Constants.PAGE_SIZE : b.getInt(4 + (n-1)*4);
        int needed = sizeOf(r);
        if (freePtr - header < needed) return false;
        // write record at freePtr - needed
        int recStart = freePtr - needed;
        writeRecord(b, recStart, r);
        // write new slot offset
        b.putInt(4 + n*4, recStart);
        // update n and free pointer (implicitly as last slot)
        b.putInt(0, n+1);
        return true;
    }
    private void writeRecord(ByteBuffer b, int pos, Record r){
        b.position(pos);
        // record = [int ncols][each value]
        b.putInt(r.values.size());
        for (int i=0;i<r.values.size();i++){
            Object v = r.values.get(i);
            if (v==null){ b.put((byte)0); continue; }
            Column.Type t = schema.get(i).type;
            if (t==Column.Type.INT){
                b.put((byte)1); b.putInt(((Number)v).intValue());
            } else {
                b.put((byte)2);
                byte[] bytes = v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                b.putInt(bytes.length);
                b.put(bytes);
            }
        }
    }
    private Record readRecord(ByteBuffer b, int pos){
        b.position(pos);
        int ncols = b.getInt();
        List<Object> vals = new ArrayList<>(ncols);
        for (int i=0;i<ncols;i++){
            byte tag = b.get();
            if (tag==0){ vals.add(null); }
            else if (tag==1){ vals.add(b.getInt()); }
            else {
                int len = b.getInt();
                byte[] d = new byte[len]; b.get(d);
                vals.add(new String(d, java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return new Record(vals);
    }
    private int sizeOf(Record r){
        int sz = 4;
        for (int i=0;i<r.values.size();i++){
            Column.Type t = schema.get(i).type;
            Object v = r.values.get(i);
            if (v==null){ sz += 1; }
            else if (t==Column.Type.INT){ sz += 1+4; }
            else {
                int len = v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                sz += 1+4+len;
            }
        }
        return sz;
    }
    public void insert(Record r){
        // try pages; otherwise allocate new
        int pages = Math.max(1, numPages());
        for (int pid=0; pid<pages; pid++){
            Page p = loadPage(pid);
            if (tryInsertInto(p, r)){ fm.writePage(tableId, p); return; }
        }
        int newPid = fm.allocatePage(tableId);
        Page p = loadPage(newPid);
        if (!tryInsertInto(p, r)) throw new DBException("Insert failed into fresh page");
        fm.writePage(tableId, p);
    }
    public Iterable<Record> scan(){
        return () -> new Iterator<Record>(){
            int pageCount = numPages();
            int page = 0;
            int idx = 0;
            Page cur = pageCount>0 ? loadPage(0) : null;
            int n=cur==null?0:cur.buf.getInt(0);
            @Override public boolean hasNext(){
                while (true){
                    if (cur==null) return false;
                    if (idx < n) return true;
                    page++;
                    if (page>=pageCount) return false;
                    cur = loadPage(page);
                    n = cur.buf.getInt(0);
                    idx = 0;
                }
            }
            @Override public Record next(){
                if (!hasNext()) throw new NoSuchElementException();
                int off = cur.buf.getInt(4 + idx*4);
                idx++;
                return readRecord(cur.buf, off);
            }
        };
    }
}

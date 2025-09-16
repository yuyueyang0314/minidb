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
    // Page layout: [int nSlots][int freePtr][slotOffsets... (negative = free)] [free space] [records]
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
    private void initIfNeeded(Page p){
        ByteBuffer b = p.buf;
        b.position(0);
        int n = b.getInt();
        if (n < 0 || n > 100000){
            b.clear();
            b.putInt(0);                 // nSlots
            b.putInt(Constants.PAGE_SIZE); // freePtr
        } else if (n==0){
            if (b.getInt(4)==0){ b.putInt(4, Constants.PAGE_SIZE); }
        }
    }
    private boolean tryInsertInto(Page p, Record r){
        initIfNeeded(p);
        ByteBuffer b = p.buf;
        int n = b.getInt(0);
        int freePtr = b.getInt(4);
        int headerBase = 8;

        // 1) 复用空槽
        for (int idx=0; idx<n; idx++){
            int off = b.getInt(headerBase + idx*4);
            if (off < 0){
                int needed = sizeOf(r);
                if (freePtr - (headerBase + n*4) < needed) return false;
                int recStart = freePtr - needed;
                writeRecord(b, recStart, r);
                b.putInt(headerBase + idx*4, recStart);
                b.putInt(4, recStart);
                return true;
            }
        }
        // 2) 追加新槽
        int needed = sizeOf(r);
        if (freePtr - (headerBase + n*4 + 4) < needed) return false;
        int recStart = freePtr - needed;
        writeRecord(b, recStart, r);
        b.putInt(headerBase + n*4, recStart);
        b.putInt(0, n+1);
        b.putInt(4, recStart);
        return true;
    }
    private void writeRecord(ByteBuffer b, int pos, Record r){
        b.position(pos);
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
    public int delete(java.util.function.Predicate<Record> pred){
        int deleted = 0;
        int pages = numPages();
        for (int pid=0; pid<pages; pid++){
            Page p = loadPage(pid);
            initIfNeeded(p);
            ByteBuffer b = p.buf;
            int n = b.getInt(0);
            int headerBase = 8;
            for (int idx=0; idx<n; idx++){
                int off = b.getInt(headerBase + idx*4);
                if (off <= 0) continue;
                Record r = readRecord(b, off);
                if (pred.test(r)){
                    b.putInt(headerBase + idx*4, -1); // tombstone
                    deleted++;
                }
            }
            if (deleted>0) fm.writePage(tableId, p);
        }
        return deleted;
    }
    public Iterable<Record> scan(){
        return () -> new Iterator<Record>(){
            int pageCount = numPages();
            int page = 0;
            int idx = 0;
            Page cur = pageCount>0 ? loadPage(0) : null;
            int n = cur==null?0:cur.buf.getInt(0);
            @Override public boolean hasNext(){
                while (true){
                    if (cur==null) return false;
                    if (idx < n){
                        int off = cur.buf.getInt(8 + idx*4);
                        if (off>0) return true;
                        idx++;
                        continue;
                    }
                    page++;
                    if (page>=pageCount) return false;
                    cur = loadPage(page);
                    n = cur.buf.getInt(0);
                    idx = 0;
                }
            }
            @Override public Record next(){
                while (true){
                    int off = cur.buf.getInt(8 + idx*4);
                    idx++;
                    if (off>0) return readRecord(cur.buf, off);
                }
            }
        };
    }
}
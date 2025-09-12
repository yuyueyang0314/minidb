package com.minidb.storage;
import java.io.*;
import java.nio.file.*;
import com.minidb.utils.*;
public class FileManager {
    private final Path dir;
    public FileManager(Path dir){
        this.dir = dir;
        try { Files.createDirectories(dir); } catch(IOException e){ throw new DBException("init file manager", e); }
    }
    private Path tablePath(int tableId){ return dir.resolve("table_"+tableId+".dat"); }
    public synchronized long fileSize(int tableId){
        try { Path p = tablePath(tableId); if (!Files.exists(p)) return 0; return Files.size(p); }
        catch(IOException e){ throw new DBException("fileSize", e); }
    }
    public synchronized Page readPage(int tableId, int pageId){
        try (RandomAccessFile raf = new RandomAccessFile(tablePath(tableId).toFile(), "rw")){
            long offset = (long)pageId * com.minidb.utils.Constants.PAGE_SIZE;
            if (raf.length() < offset + com.minidb.utils.Constants.PAGE_SIZE){
                byte[] zero = new byte[com.minidb.utils.Constants.PAGE_SIZE];
                return new Page(pageId, zero);
            }
            raf.seek(offset);
            byte[] data = new byte[com.minidb.utils.Constants.PAGE_SIZE];
            raf.readFully(data);
            return new Page(pageId, data);
        }catch(IOException e){ throw new DBException("readPage", e); }
    }
    public synchronized void writePage(int tableId, Page page){
        try (RandomAccessFile raf = new RandomAccessFile(tablePath(tableId).toFile(), "rw")){
            long offset = (long)page.pageId * com.minidb.utils.Constants.PAGE_SIZE;
            raf.seek(offset);
            raf.write(page.buf.array());
        }catch(IOException e){ throw new DBException("writePage", e); }
    }
    public synchronized int allocatePage(int tableId){
        long size = fileSize(tableId);
        int nextPageId = (int)(size / com.minidb.utils.Constants.PAGE_SIZE);
        writePage(tableId, new Page(nextPageId, new byte[com.minidb.utils.Constants.PAGE_SIZE]));
        return nextPageId;
    }
}

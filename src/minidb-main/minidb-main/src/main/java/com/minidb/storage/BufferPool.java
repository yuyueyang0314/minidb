package com.minidb.storage;
import java.util.*;
public class BufferPool {
    private final int capacity;
    private final LinkedHashMap<String, Page> cache;
    public BufferPool(int capacity){
        this.capacity = capacity;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true){
            protected boolean removeEldestEntry(Map.Entry<String, Page> eldest){
                return size() > BufferPool.this.capacity;
            }
        };
    }
    private String key(int tableId, int pageId){ return tableId+":"+pageId; }
    public synchronized Page get(int tableId, int pageId){
        return cache.get(key(tableId, pageId));
    }
    public synchronized void put(int tableId, int pageId, Page p){
        cache.put(key(tableId, pageId), p);
    }
    public synchronized void clear(){ cache.clear(); }
}

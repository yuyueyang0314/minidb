package com.minidb.storage;
import java.util.*;
public class BufferPool {
    public enum Policy { LRU, FIFO }
    private final int capacity;
    private final Policy policy;
    private long hits=0, misses=0, puts=0, evicts=0;
    private final Map<String, Page> cache;
    private final Queue<String> fifoQ;

    public BufferPool(int capacity){ this(capacity, Policy.LRU); }
    public BufferPool(int capacity, Policy policy){
        this.capacity = capacity;
        this.policy = policy;
        if (policy==Policy.LRU){
            this.cache = new LinkedHashMap<>(capacity, 0.75f, true){
                protected boolean removeEldestEntry(Map.Entry<String, Page> eldest){
                    boolean rm = size() > BufferPool.this.capacity;
                    if (rm) evicts++;
                    return rm;
                }
            };
            this.fifoQ = null;
        } else {
            this.cache = new HashMap<>();
            this.fifoQ = new ArrayDeque<>();
        }
    }
    private String key(int tableId, int pageId){ return tableId+":"+pageId; }
    public synchronized Page get(int tableId, int pageId){
        String k = key(tableId, pageId);
        Page p = cache.get(k);
        if (p==null){ misses++; } else { hits++; }
        return p;
    }
    public synchronized void put(int tableId, int pageId, Page p){
        String k = key(tableId, pageId);
        if (policy==Policy.FIFO){
            if (!cache.containsKey(k)){
                if (cache.size() >= capacity){
                    String victim = fifoQ.poll();
                    if (victim!=null){ cache.remove(victim); evicts++; }
                }
                fifoQ.add(k);
            }
            cache.put(k, p);
        } else {
            cache.put(k, p);
        }
        puts++;
    }
    public synchronized void clear(){ cache.clear(); if (fifoQ!=null) fifoQ.clear(); }
    public synchronized long hits(){ return hits; }
    public synchronized long misses(){ return misses; }
    public synchronized long evicts(){ return evicts; }
    public synchronized long size(){ return cache.size(); }
    public Policy policy(){ return policy; }
}

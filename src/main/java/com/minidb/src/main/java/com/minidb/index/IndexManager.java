package com.minidb.index;

import com.minidb.catalog.*;
import com.minidb.storage.*;
import com.minidb.utils.*;
import java.util.*;
import java.nio.file.*;

public class IndexManager {
    private final Map<String, BPlusTree> indexes = new HashMap<>();
    private final Catalog catalog;
    private final FileManager fm;
    private final BufferPool bp;
    private final Path indexDir;
    
    public IndexManager(Catalog catalog, FileManager fm, BufferPool bp) {
        this.catalog = catalog;
        this.fm = fm;
        this.bp = bp;
        this.indexDir = fm.dir().resolve("indexes");
        try {
            Files.createDirectories(indexDir);
        } catch (Exception e) {
            throw new DBException("Failed to create index directory", e);
        }
    }
    
    public void createIndex(String indexName, String tableName, String columnName) {
        // 检查表是否存在
        TableInfo table = catalog.getTable(tableName);
        
        // 检查列是否存在
        if (table.schema.indexOf(columnName).isEmpty()) {
            throw new DBException("Column not found: " + columnName);
        }
        
        // 检查索引是否已存在
        if (indexes.containsKey(indexName)) {
            throw new DBException("Index already exists: " + indexName);
        }
        
        // 创建B+树索引
        BPlusTree index = new BPlusTree(table.tableId, columnName, fm, bp);
        indexes.put(indexName, index);
        
        // 构建索引（扫描表数据）
        buildIndex(index, table, columnName);
        
        // 保存索引元数据
        saveIndexMetadata(indexName, tableName, columnName);
    }
    
    public void dropIndex(String indexName) {
        BPlusTree index = indexes.remove(indexName);
        if (index == null) {
            throw new DBException("Index not found: " + indexName);
        }
        
        // 删除索引元数据
        deleteIndexMetadata(indexName);
    }
    
    public List<Integer> search(String indexName, Object key) {
        BPlusTree index = indexes.get(indexName);
        if (index == null) {
            throw new DBException("Index not found: " + indexName);
        }
        return index.search(key);
    }
    
    public List<Integer> rangeSearch(String indexName, Object minKey, Object maxKey) {
        BPlusTree index = indexes.get(indexName);
        if (index == null) {
            throw new DBException("Index not found: " + indexName);
        }
        return index.rangeSearch(minKey, maxKey);
    }
    
    public void insert(String indexName, Object key, int recordId) {
        BPlusTree index = indexes.get(indexName);
        if (index != null) {
            index.insert(key, recordId);
        }
    }
    
    public void delete(String indexName, Object key) {
        BPlusTree index = indexes.get(indexName);
        if (index != null) {
            index.delete(key);
        }
    }
    
    public boolean hasIndex(String indexName) {
        return indexes.containsKey(indexName);
    }
    
    public List<String> getIndexNames() {
        return new ArrayList<>(indexes.keySet());
    }
    
    public List<String> getIndexesForTable(String tableName) {
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, BPlusTree> entry : indexes.entrySet()) {
            // 这里简化实现，实际应该从元数据中查找
            result.add(entry.getKey());
        }
        return result;
    }
    
    private void buildIndex(BPlusTree index, TableInfo table, String columnName) {
        TableHeap heap = new TableHeap(table.tableId, table.schema, fm, bp);
        int columnIndex = table.schema.indexOf(columnName).orElseThrow();
        
        int recordId = 0;
        for (com.minidb.storage.Record record : heap.scan()) {
            Object key = record.values.get(columnIndex);
            index.insert(key, recordId++);
        }
    }
    
    private void saveIndexMetadata(String indexName, String tableName, String columnName) {
        // 简化实现，实际应该保存到文件中
        // 这里可以保存到catalog.meta文件中
    }
    
    private void deleteIndexMetadata(String indexName) {
        // 简化实现，实际应该从文件中删除
    }
    
    public void loadIndexes() {
        // 从元数据文件加载所有索引
        // 简化实现
    }
}

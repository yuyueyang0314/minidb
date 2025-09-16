package com.minidb.transaction;

import com.minidb.storage.*;
import com.minidb.utils.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private final FileManager fm;
    private final BufferPool bp;
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicLong nextTransactionId = new AtomicLong(1);
    private final Map<String, List<LogEntry>> transactionLog = new ConcurrentHashMap<>();
    
    public TransactionManager(FileManager fm, BufferPool bp) {
        this.fm = fm;
        this.bp = bp;
    }
    
    public long beginTransaction() {
        long transactionId = nextTransactionId.getAndIncrement();
        Transaction transaction = new Transaction(transactionId);
        activeTransactions.put(transactionId, transaction);
        
        // 记录事务开始日志
        logTransaction(transactionId, "BEGIN", null, null, null);
        
        return transactionId;
    }
    
    public void commitTransaction(long transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new DBException("Transaction not found: " + transactionId);
        }
        
        try {
            // 执行所有待提交的操作
            for (LogEntry entry : transaction.getLogEntries()) {
                executeLogEntry(entry);
            }
            
            // 记录事务提交日志
            logTransaction(transactionId, "COMMIT", null, null, null);
            
            // 清理事务
            activeTransactions.remove(transactionId);
            
        } catch (Exception e) {
            // 回滚事务
            rollbackTransaction(transactionId);
            throw new DBException("Transaction commit failed", e);
        }
    }
    
    public void rollbackTransaction(long transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new DBException("Transaction not found: " + transactionId);
        }
        
        // 回滚所有操作
        List<LogEntry> entries = new ArrayList<>(transaction.getLogEntries());
        Collections.reverse(entries); // 逆序执行回滚
        
        for (LogEntry entry : entries) {
            rollbackLogEntry(entry);
        }
        
        // 记录事务回滚日志
        logTransaction(transactionId, "ROLLBACK", null, null, null);
        
        // 清理事务
        activeTransactions.remove(transactionId);
    }
    
    public void logOperation(long transactionId, String operation, String tableName, 
                           Map<String, Object> oldValues, Map<String, Object> newValues) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new DBException("Transaction not found: " + transactionId);
        }
        
        LogEntry entry = new LogEntry(transactionId, operation, tableName, oldValues, newValues);
        transaction.addLogEntry(entry);
        logTransaction(transactionId, operation, tableName, oldValues, newValues);
    }
    
    private void logTransaction(long transactionId, String operation, String tableName,
                              Map<String, Object> oldValues, Map<String, Object> newValues) {
        LogEntry entry = new LogEntry(transactionId, operation, tableName, oldValues, newValues);
        transactionLog.computeIfAbsent(String.valueOf(transactionId), k -> new ArrayList<>()).add(entry);
    }
    
    private void executeLogEntry(LogEntry entry) {
        // 实际执行操作
        // 这里简化实现，实际应该根据操作类型执行相应的数据库操作
    }
    
    private void rollbackLogEntry(LogEntry entry) {
        // 回滚操作
        // 这里简化实现，实际应该根据操作类型执行相应的回滚操作
    }
    
    public boolean isTransactionActive(long transactionId) {
        return activeTransactions.containsKey(transactionId);
    }
    
    public List<Long> getActiveTransactionIds() {
        return new ArrayList<>(activeTransactions.keySet());
    }
    
    public void cleanup() {
        // 清理所有活动事务
        for (Long transactionId : new ArrayList<>(activeTransactions.keySet())) {
            rollbackTransaction(transactionId);
        }
    }
    
    // 事务类
    public static class Transaction {
        private final long transactionId;
        private final List<LogEntry> logEntries = new ArrayList<>();
        private final long startTime;
        
        public Transaction(long transactionId) {
            this.transactionId = transactionId;
            this.startTime = System.currentTimeMillis();
        }
        
        public long getTransactionId() { return transactionId; }
        public long getStartTime() { return startTime; }
        
        public void addLogEntry(LogEntry entry) {
            logEntries.add(entry);
        }
        
        public List<LogEntry> getLogEntries() {
            return new ArrayList<>(logEntries);
        }
    }
    
    // 日志条目类
    public static class LogEntry {
        private final long transactionId;
        private final String operation;
        private final String tableName;
        private final Map<String, Object> oldValues;
        private final Map<String, Object> newValues;
        private final long timestamp;
        
        public LogEntry(long transactionId, String operation, String tableName,
                       Map<String, Object> oldValues, Map<String, Object> newValues) {
            this.transactionId = transactionId;
            this.operation = operation;
            this.tableName = tableName;
            this.oldValues = oldValues != null ? new HashMap<>(oldValues) : null;
            this.newValues = newValues != null ? new HashMap<>(newValues) : null;
            this.timestamp = System.currentTimeMillis();
        }
        
        public long getTransactionId() { return transactionId; }
        public String getOperation() { return operation; }
        public String getTableName() { return tableName; }
        public Map<String, Object> getOldValues() { return oldValues; }
        public Map<String, Object> getNewValues() { return newValues; }
        public long getTimestamp() { return timestamp; }
    }
}

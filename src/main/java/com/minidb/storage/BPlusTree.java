package com.minidb.storage;

import com.minidb.utils.*;
import java.util.*;
import java.nio.ByteBuffer;

public class BPlusTree {
    private final int tableId;
    private final String columnName;
    private final FileManager fm;
    private final BufferPool bp;
    private final int order; // B+树的阶数
    private int rootPageId;
    
    public BPlusTree(int tableId, String columnName, FileManager fm, BufferPool bp) {
        this.tableId = tableId;
        this.columnName = columnName;
        this.fm = fm;
        this.bp = bp;
        this.order = 4; // 4阶B+树
        this.rootPageId = -1; // 初始时没有根节点
    }
    
    public void insert(Object key, int recordId) {
        if (rootPageId == -1) {
            // 创建根节点
            rootPageId = fm.allocatePage(tableId);
            LeafNode root = new LeafNode(rootPageId);
            root.insert(key, recordId);
            writeNode(root);
        } else {
            // 从根节点开始插入
            Node root = readNode(rootPageId);
            InsertResult result = root.insert(key, recordId);
            if (result != null) {
                // 需要分裂根节点
                InternalNode newRoot = new InternalNode(fm.allocatePage(tableId));
                newRoot.keys.add(result.key);
                newRoot.children.add(rootPageId);
                newRoot.children.add(result.newNodeId);
                rootPageId = newRoot.pageId;
                writeNode(newRoot);
            }
        }
    }
    
    public void delete(Object key) {
        if (rootPageId != -1) {
            Node root = readNode(rootPageId);
            root.delete(key);
            if (root.isEmpty() && root instanceof InternalNode) {
                // 根节点变空，更新根节点
                InternalNode internalRoot = (InternalNode) root;
                if (internalRoot.children.size() == 1) {
                    rootPageId = internalRoot.children.get(0);
                }
            }
        }
    }
    
    public List<Integer> search(Object key) {
        if (rootPageId == -1) return List.of();
        
        Node current = readNode(rootPageId);
        while (current instanceof InternalNode) {
            InternalNode internal = (InternalNode) current;
            int childIndex = findChildIndex(internal, key);
            current = readNode(internal.children.get(childIndex));
        }
        
        LeafNode leaf = (LeafNode) current;
        return leaf.search(key);
    }
    
    public List<Integer> rangeSearch(Object minKey, Object maxKey) {
        if (rootPageId == -1) return List.of();
        
        Node current = readNode(rootPageId);
        while (current instanceof InternalNode) {
            InternalNode internal = (InternalNode) current;
            int childIndex = findChildIndex(internal, minKey);
            current = readNode(internal.children.get(childIndex));
        }
        
        LeafNode leaf = (LeafNode) current;
        return leaf.rangeSearch(minKey, maxKey);
    }
    
    private int findChildIndex(InternalNode node, Object key) {
        for (int i = 0; i < node.keys.size(); i++) {
            if (compare(key, node.keys.get(i)) < 0) {
                return i;
            }
        }
        return node.keys.size();
    }
    
    private int compare(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number)a).doubleValue(), ((Number)b).doubleValue());
        }
        return a.toString().compareTo(b.toString());
    }
    
    private Node readNode(int pageId) {
        Page page = bp.get(tableId, pageId);
        if (page == null) {
            page = fm.readPage(tableId, pageId);
            bp.put(tableId, pageId, page);
        }
        
        ByteBuffer buf = page.buf;
        buf.position(0);
        boolean isLeaf = buf.get() == 1;
        
        if (isLeaf) {
            return new LeafNode(pageId, buf);
        } else {
            return new InternalNode(pageId, buf);
        }
    }
    
    private void writeNode(Node node) {
        Page page = new Page(node.pageId, new byte[Constants.PAGE_SIZE]);
        ByteBuffer buf = page.buf;
        
        if (node instanceof LeafNode) {
            buf.put((byte) 1); // isLeaf = true
            LeafNode leaf = (LeafNode) node;
            buf.putInt(leaf.keys.size());
            for (int i = 0; i < leaf.keys.size(); i++) {
                writeObject(buf, leaf.keys.get(i));
                buf.putInt(leaf.recordIds.get(i));
            }
        } else {
            buf.put((byte) 0); // isLeaf = false
            InternalNode internal = (InternalNode) node;
            buf.putInt(internal.keys.size());
            for (Object key : internal.keys) {
                writeObject(buf, key);
            }
            buf.putInt(internal.children.size());
            for (int childId : internal.children) {
                buf.putInt(childId);
            }
        }
        
        fm.writePage(tableId, page);
    }
    
    private void writeObject(ByteBuffer buf, Object obj) {
        if (obj == null) {
            buf.put((byte) 0);
        } else if (obj instanceof Number) {
            buf.put((byte) 1);
            buf.putInt(((Number)obj).intValue());
        } else {
            buf.put((byte) 2);
            byte[] bytes = obj.toString().getBytes();
            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }
    
    private Object readObject(ByteBuffer buf) {
        byte type = buf.get();
        switch (type) {
            case 0: return null;
            case 1: return buf.getInt();
            case 2: 
                int len = buf.getInt();
                byte[] bytes = new byte[len];
                buf.get(bytes);
                return new String(bytes);
            default: throw new DBException("Unknown object type: " + type);
        }
    }
    
    // 抽象节点类
    abstract static class Node {
        final int pageId;
        
        Node(int pageId) {
            this.pageId = pageId;
        }
        
        abstract InsertResult insert(Object key, int recordId);
        abstract void delete(Object key);
        abstract boolean isEmpty();
    }
    
    // 内部节点
    class InternalNode extends Node {
        List<Object> keys = new ArrayList<>();
        List<Integer> children = new ArrayList<>();
        
        InternalNode(int pageId) {
            super(pageId);
        }
        
        InternalNode(int pageId, ByteBuffer buf) {
            super(pageId);
            int keyCount = buf.getInt();
            for (int i = 0; i < keyCount; i++) {
                keys.add(readObjectFromBuffer(buf));
            }
            int childCount = buf.getInt();
            for (int i = 0; i < childCount; i++) {
                children.add(buf.getInt());
            }
        }
        
        @Override
        InsertResult insert(Object key, int recordId) {
            // 简化实现，实际应该递归处理
            return null;
        }
        
        @Override
        void delete(Object key) {
            // 简化实现
        }
        
        @Override
        boolean isEmpty() {
            return children.isEmpty();
        }
    }
    
    // 叶子节点
    class LeafNode extends Node {
        List<Object> keys = new ArrayList<>();
        List<Integer> recordIds = new ArrayList<>();
        
        LeafNode(int pageId) {
            super(pageId);
        }
        
        LeafNode(int pageId, ByteBuffer buf) {
            super(pageId);
            int count = buf.getInt();
            for (int i = 0; i < count; i++) {
                keys.add(readObjectFromBuffer(buf));
                recordIds.add(buf.getInt());
            }
        }
        
        @Override
        InsertResult insert(Object key, int recordId) {
            int pos = Collections.binarySearch(keys, key, BPlusTree::compareObjects);
            if (pos < 0) pos = -pos - 1;
            
            keys.add(pos, key);
            recordIds.add(pos, recordId);
            
            if (keys.size() > order) {
                // 需要分裂
                int mid = keys.size() / 2;
                LeafNode newLeaf = new LeafNode(fm.allocatePage(tableId));
                newLeaf.keys = new ArrayList<>(keys.subList(mid, keys.size()));
                newLeaf.recordIds = new ArrayList<>(recordIds.subList(mid, recordIds.size()));
                
                keys = new ArrayList<>(keys.subList(0, mid));
                recordIds = new ArrayList<>(recordIds.subList(0, mid));
                
                return new InsertResult(newLeaf.keys.get(0), newLeaf.pageId);
            }
            return null;
        }
        
        @Override
        void delete(Object key) {
            int pos = Collections.binarySearch(keys, key, BPlusTree::compareObjects);
            if (pos >= 0) {
                keys.remove(pos);
                recordIds.remove(pos);
            }
        }
        
        @Override
        boolean isEmpty() {
            return keys.isEmpty();
        }
        
        List<Integer> search(Object key) {
            int pos = Collections.binarySearch(keys, key, BPlusTree::compareObjects);
            if (pos >= 0) {
                return List.of(recordIds.get(pos));
            }
            return List.of();
        }
        
        List<Integer> rangeSearch(Object minKey, Object maxKey) {
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                Object key = keys.get(i);
                if (BPlusTree.compareObjects(key, minKey) >= 0 && BPlusTree.compareObjects(key, maxKey) <= 0) {
                    result.add(recordIds.get(i));
                }
            }
            return result;
        }
    }
    
    // 插入结果
    class InsertResult {
        final Object key;
        final int newNodeId;
        
        InsertResult(Object key, int newNodeId) {
            this.key = key;
            this.newNodeId = newNodeId;
        }
    }
    
    private static int compareObjects(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number)a).doubleValue(), ((Number)b).doubleValue());
        }
        return a.toString().compareTo(b.toString());
    }
    
    private static Object readObjectFromBuffer(ByteBuffer buf) {
        byte type = buf.get();
        switch (type) {
            case 0: return null;
            case 1: return buf.getInt();
            case 2: 
                int len = buf.getInt();
                byte[] bytes = new byte[len];
                buf.get(bytes);
                return new String(bytes);
            default: throw new DBException("Unknown object type: " + type);
        }
    }
}

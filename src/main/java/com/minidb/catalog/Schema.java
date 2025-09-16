package com.minidb.catalog;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据库表模式定义
 * 
 * 表示数据库表的结构，包含所有列的定义信息
 * 提供列查找、验证等操作
 */
public class Schema {
    
    /** 列定义列表（不可变） */
    private final List<Column> columns;
    
    /** 列名到索引的映射（用于快速查找） */
    private final Map<String, Integer> columnIndexMap;
    
    /**
     * 创建表模式
     * 
     * @param columns 列定义列表
     * @throws IllegalArgumentException 如果列列表为空或包含重复列名
     */
    public Schema(List<Column> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Schema must have at least one column");
        }
        
        // 检查列名是否重复
        Set<String> columnNames = new HashSet<>();
        for (Column column : columns) {
            String name = column.name.toLowerCase();
            if (!columnNames.add(name)) {
                throw new IllegalArgumentException("Duplicate column name: " + column.name);
            }
        }
        
        this.columns = List.copyOf(columns);
        this.columnIndexMap = createColumnIndexMap();
    }
    
    /**
     * 创建列名到索引的映射
     * 
     * @return 列名映射
     */
    private Map<String, Integer> createColumnIndexMap() {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i).name.toLowerCase(), i);
        }
        return map;
    }
    
    /**
     * 获取列的数量
     * 
     * @return 列的数量
     */
    public int size() {
        return columns.size();
    }
    
    /**
     * 根据索引获取列定义
     * 
     * @param index 列索引
     * @return 列定义
     * @throws IndexOutOfBoundsException 如果索引超出范围
     */
    public Column get(int index) {
        return columns.get(index);
    }
    
    /**
     * 获取所有列定义
     * 
     * @return 列定义列表（不可变）
     */
    public List<Column> columns() {
        return columns;
    }
    
    /**
     * 根据列名查找列索引
     * 
     * @param columnName 列名（不区分大小写）
     * @return 列索引，如果不存在返回空Optional
     */
    public OptionalInt indexOf(String columnName) {
        if (columnName == null) {
            return OptionalInt.empty();
        }
        
        Integer index = columnIndexMap.get(columnName.toLowerCase());
        return index != null ? OptionalInt.of(index) : OptionalInt.empty();
    }
    
    /**
     * 根据列名获取列定义
     * 
     * @param columnName 列名（不区分大小写）
     * @return 列定义，如果不存在返回空Optional
     */
    public Optional<Column> getColumn(String columnName) {
        OptionalInt index = indexOf(columnName);
        return index.isPresent() ? Optional.of(columns.get(index.getAsInt())) : Optional.empty();
    }
    
    /**
     * 检查是否包含指定列
     * 
     * @param columnName 列名（不区分大小写）
     * @return 如果包含返回true，否则返回false
     */
    public boolean hasColumn(String columnName) {
        return columnIndexMap.containsKey(columnName.toLowerCase());
    }
    
    /**
     * 获取所有列名
     * 
     * @return 列名列表
     */
    public List<String> getColumnNames() {
        return columns.stream()
                     .map(column -> column.name)
                     .collect(Collectors.toList());
    }
    
    /**
     * 获取指定类型的列
     * 
     * @param type 列类型
     * @return 指定类型的列列表
     */
    public List<Column> getColumnsByType(Column.Type type) {
        return columns.stream()
                     .filter(column -> column.type == type)
                     .collect(Collectors.toList());
    }
    
    /**
     * 验证值列表是否与模式匹配
     * 
     * @param values 值列表
     * @return 如果匹配返回true，否则返回false
     */
    public boolean validateValues(List<Object> values) {
        if (values == null || values.size() != columns.size()) {
            return false;
        }
        
        for (int i = 0; i < columns.size(); i++) {
            if (!columns.get(i).isCompatibleWith(values.get(i))) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 转换值列表以匹配模式
     * 
     * @param values 原始值列表
     * @return 转换后的值列表
     * @throws IllegalArgumentException 如果值无法转换
     */
    public List<Object> convertValues(List<Object> values) {
        if (values == null || values.size() != columns.size()) {
            throw new IllegalArgumentException(
                String.format("Expected %d values, got %d", columns.size(), 
                             values != null ? values.size() : 0));
        }
        
        List<Object> convertedValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            convertedValues.add(columns.get(i).convertValue(values.get(i)));
        }
        
        return convertedValues;
    }
    
    /**
     * 创建模式的副本
     * 
     * @return 新的模式实例
     */
    public Schema copy() {
        return new Schema(new ArrayList<>(columns));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        Schema other = (Schema) obj;
        return columns.equals(other.columns);
    }
    
    @Override
    public int hashCode() {
        return columns.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("Schema{columns=%s}", columns);
    }
}

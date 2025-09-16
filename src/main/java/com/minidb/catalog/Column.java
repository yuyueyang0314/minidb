package com.minidb.catalog;

/**
 * 数据库列定义
 * 
 * 表示数据库表中的一个列，包含列名和数据类型信息
 */
public class Column {
    
    /**
     * 列的数据类型枚举
     */
    public enum Type { 
        /** 整数类型 */
        INT, 
        /** 文本类型 */
        TEXT 
    }
    
    /** 列名 */
    public final String name;
    /** 列的数据类型 */
    public final Type type;
    
    /**
     * 创建列定义
     * 
     * @param name 列名
     * @param type 数据类型
     * @throws IllegalArgumentException 如果列名为空或null
     */
    public Column(String name, Type type) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        this.name = name.trim();
        this.type = type;
    }
    
    /**
     * 检查列是否支持指定的值
     * 
     * @param value 要检查的值
     * @return 如果值类型匹配列类型返回true，否则返回false
     */
    public boolean isCompatibleWith(Object value) {
        if (value == null) {
            return true; // 所有列都支持NULL值
        }
        
        return switch (type) {
            case INT -> value instanceof Number;
            case TEXT -> true; // 所有值都可以转换为文本
        };
    }
    
    /**
     * 将值转换为列类型
     * 
     * @param value 要转换的值
     * @return 转换后的值
     * @throws IllegalArgumentException 如果值无法转换为列类型
     */
    public Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        
        return switch (type) {
            case INT -> {
                if (value instanceof Number) {
                    yield ((Number) value).intValue();
                }
                try {
                    yield Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        String.format("Cannot convert '%s' to INT for column '%s'", value, name), e);
                }
            }
            case TEXT -> value.toString();
        };
    }
    
    /**
     * 获取类型的字符串表示
     * 
     * @return 类型名称
     */
    public String getTypeName() {
        return type.name();
    }
    
    /**
     * 检查是否为整数类型
     * 
     * @return 如果是整数类型返回true，否则返回false
     */
    public boolean isIntegerType() {
        return type == Type.INT;
    }
    
    /**
     * 检查是否为文本类型
     * 
     * @return 如果是文本类型返回true，否则返回false
     */
    public boolean isTextType() {
        return type == Type.TEXT;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        Column other = (Column) obj;
        return name.equals(other.name) && type == other.type;
    }
    
    @Override
    public int hashCode() {
        return name.hashCode() * 31 + type.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("Column{name='%s', type=%s}", name, type);
    }
}

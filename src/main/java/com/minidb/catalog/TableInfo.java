package com.minidb.catalog;
public class TableInfo {
    public final String name;
    public final Schema schema;
    public final int tableId;
    public TableInfo(String name, Schema schema, int tableId){
        this.name = name;
        this.schema = schema;
        this.tableId = tableId;
    }
}

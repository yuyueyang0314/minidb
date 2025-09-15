package com.minidb.catalog;
import java.util.*;
public class Schema {
    private final List<Column> columns;
    public Schema(List<Column> cols){ this.columns = List.copyOf(cols); }
    public int size(){ return columns.size(); }
    public Column get(int i){ return columns.get(i); }
    public List<Column> columns(){ return columns; }
    public OptionalInt indexOf(String name){
        for (int i=0;i<columns.size();i++)
            if (columns.get(i).name.equalsIgnoreCase(name)) return OptionalInt.of(i);
        return OptionalInt.empty();
    }
}

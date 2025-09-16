package com.minidb.catalog;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import com.minidb.utils.*;
public class Catalog {
    private final Map<String, TableInfo> tablesByName = new HashMap<>();
    private final Map<Integer, TableInfo> tablesById = new HashMap<>();
    private int nextTableId = 1;
    private final Path metaFile;
    public Catalog(Path dbDir){
        this.metaFile = dbDir.resolve("catalog.meta");
        load();
    }
    public synchronized TableInfo createTable(String name, Schema schema){
        if (tablesByName.containsKey(name.toLowerCase()))
            throw new DBException("Table exists: " + name);
        TableInfo t = new TableInfo(name, schema, nextTableId++);
        tablesByName.put(name.toLowerCase(), t);
        tablesById.put(t.tableId, t);
        persist();
        return t;
    }
    public synchronized TableInfo getTable(String name){
        TableInfo t = tablesByName.get(name.toLowerCase());
        if (t==null) throw new DBException("Unknown table: "+name);
        return t;
    }
    public synchronized Collection<TableInfo> allTables(){
        return Collections.unmodifiableCollection(tablesByName.values());
    }
    private void persist(){
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(metaFile))){
            oos.writeInt(nextTableId);
            oos.writeInt(tablesById.size());
            for (TableInfo t: tablesById.values()){
                oos.writeInt(t.tableId);
                oos.writeUTF(t.name);
                // write schema
                oos.writeInt(t.schema.size());
                for (Column c: t.schema.columns()){
                    oos.writeUTF(c.name);
                    oos.writeUTF(c.type.name());
                }
            }
        }catch(IOException e){
            throw new DBException("Persist catalog failed", e);
        }
    }
    private void load(){
        try {
            if (!Files.exists(metaFile)) return;
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(metaFile))){
                nextTableId = ois.readInt();
                int n = ois.readInt();
                for (int i=0;i<n;i++){
                    int id = ois.readInt();
                    String name = ois.readUTF();
                    int m = ois.readInt();
                    List<Column> cols = new ArrayList<>();
                    for (int j=0;j<m;j++){
                        String cn = ois.readUTF();
                        Column.Type tp = Column.Type.valueOf(ois.readUTF());
                        cols.add(new Column(cn, tp));
                    }
                    TableInfo t = new TableInfo(name, new Schema(cols), id);
                    tablesByName.put(name.toLowerCase(), t);
                    tablesById.put(id, t);
                }
            }
        }catch(IOException e){
            throw new DBException("Load catalog failed", e);
        }
    }
    public synchronized void dropTable(String name){
        TableInfo t = tablesByName.remove(name.toLowerCase());
        if (t==null) throw new com.minidb.utils.DBException("Unknown table: "+name);
        tablesById.remove(t.tableId);
        persist(); // 复用 v3 里的持久化
    }
}

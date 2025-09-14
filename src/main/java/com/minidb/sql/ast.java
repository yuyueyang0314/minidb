package com.minidb.sql;
import java.util.*;
public class ast {
    public interface Stmt {}
    public static class CreateTable implements Stmt {
        public final String name; public final List<Col> cols;
        public CreateTable(String name, List<Col> cols){ this.name=name; this.cols=cols; }
        public static class Col { public final String name, type; public Col(String n,String t){name=n;type=t;} }
    }
    public static class Insert implements Stmt {
        public final String table; public final List<List<Object>> rows;
        public Insert(String table, List<List<Object>> rows){ this.table=table; this.rows=rows; }
    }
    public static class Select implements Stmt {
        public final String table; public final List<String> cols; public final Cond where;
        public Select(String table, List<String> cols, Cond where){ this.table=table; this.cols=cols; this.where=where; }
    }
    public static class Cond {
        public final String col; public final String op; public final Object lit;
        public Cond(String c,String o,Object l){ col=c;op=o;lit=l; }
    }
}

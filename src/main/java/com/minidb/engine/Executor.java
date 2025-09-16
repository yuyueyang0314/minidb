package com.minidb.engine;
import java.util.*;
import com.minidb.sql.*;
import com.minidb.catalog.*;
import com.minidb.storage.*;
import com.minidb.utils.*;
import com.minidb.engine.plan.*;
import com.minidb.transaction.*;
import com.minidb.index.*;

public class Executor {
    private final Catalog catalog;
    private final FileManager fm;
    private final BufferPool bp;
    private final SemanticAnalyzer semanticAnalyzer;
    private final TransactionManager transactionManager;
    private final IndexManager indexManager;
    private long currentTransactionId = -1;

    public Executor(Catalog catalog, FileManager fm, BufferPool bp){
        this.catalog=catalog; this.fm=fm; this.bp=bp;
        this.semanticAnalyzer = new SemanticAnalyzer(catalog);
        this.transactionManager = new TransactionManager(fm, bp);
        this.indexManager = new IndexManager(catalog, fm, bp);
    }

    public Result exec(ast.Stmt stmt){
        // Perform semantic analysis first
        try {
            semanticAnalyzer.analyze(stmt);
        } catch (SemanticAnalyzer.SemanticException e) {
            return Result.error("Semantic error: " + e.getMessage());
        }
        
        if (stmt instanceof ast.CreateTable ct) return doCreate(ct);
        if (stmt instanceof ast.DropTable  dt) return doDrop(dt);
        if (stmt instanceof ast.Insert     in) return doInsert(in);
        if (stmt instanceof ast.Update     up) return doUpdate(up);
        if (stmt instanceof ast.Delete     de) return doDelete(de);
        if (stmt instanceof ast.Select     se) return doSelect(se);
        if (stmt instanceof ast.CreateIndex ci) return doCreateIndex(ci);
        if (stmt instanceof ast.DropIndex  di) return doDropIndex(di);
        if (stmt instanceof ast.BeginTransaction bt) return doBeginTransaction(bt);
        if (stmt instanceof ast.CommitTransaction ct) return doCommitTransaction(ct);
        if (stmt instanceof ast.RollbackTransaction rt) return doRollbackTransaction(rt);
        throw new DBException("Unsupported statement");
    }

    private Result doCreate(ast.CreateTable ct){
        List<Column> cols = new ArrayList<>();
        for (var c: ct.cols) cols.add(new Column(c.name, "INT".equalsIgnoreCase(c.type) ? Column.Type.INT : Column.Type.TEXT));
        Schema schema = new Schema(cols);
        TableInfo t = catalog.createTable(ct.name, schema);
        fm.allocatePage(t.tableId);
        return Result.message("Table created: "+ct.name);
    }

    private Result doDrop(ast.DropTable dt){
        TableInfo t = catalog.getTable(dt.table);
        fm.deleteTable(t.tableId);
        catalog.dropTable(dt.table);
        return Result.message("Table dropped: "+dt.table);
    }

    private Result doInsert(ast.Insert ins){
        TableInfo t = catalog.getTable(ins.table);
        TableHeap heap = new TableHeap(t.tableId, t.schema, fm, bp);
        for (var row: ins.rows){
            if (row.size()!=t.schema.size()) throw new DBException("Column count mismatch");
            heap.insert(new com.minidb.storage.Record(row));
        }
        return Result.message("Inserted "+ins.rows.size()+" row(s).");
    }

    private Result doUpdate(ast.Update up){
        TableInfo t = catalog.getTable(up.table);
        TableHeap heap = new TableHeap(t.tableId, t.schema, fm, bp);
        java.util.function.Predicate<com.minidb.storage.Record> pred =
                (up.where == null) ? (r -> true) : (r -> evalBool(up.where, t.schema, r));

        java.util.function.Function<com.minidb.storage.Record, com.minidb.storage.Record> transformer =
                (r) -> {
                    List<Object> vals = new ArrayList<>(r.values);
                    for (var sc : up.sets) {
                        int idx = t.schema.indexOf(sc.col).orElseThrow(() -> new DBException("Unknown column " + sc.col));
                        Object v = eval(sc.expr, t.schema, r);
                        v = castTo(t.schema.get(idx).type, v);
                        vals.set(idx, v);
                    }
                    return new com.minidb.storage.Record(vals); // 返回更新后的记录
                };
        int n = heap.update(pred, transformer);
        return Result.message("Updated "+n+" row(s).");
    }

    private Result doDelete(ast.Delete del){
        TableInfo t = catalog.getTable(del.table);
        TableHeap heap = new TableHeap(t.tableId, t.schema, fm, bp);
        java.util.function.Predicate<com.minidb.storage.Record> pred =
                (del.where==null) ? r -> true : (r -> evalBool(del.where, t.schema, r));
        int n = heap.delete(pred);
        return Result.message("Deleted "+n+" row(s).");
    }

    private Result doSelect(ast.Select sel){
        LogicalPlan plan = Planner.plan(sel); // 结构化
        TableInfo t = catalog.getTable(sel.table);
        TableHeap heap = new TableHeap(t.tableId, t.schema, fm, bp);

        List<Integer> projIdx = new ArrayList<>();
        if (sel.cols.size()==1 && sel.cols.get(0).equals("*")){
            for (int i=0;i<t.schema.size();i++) projIdx.add(i);
        } else {
            for (String c: sel.cols){
                int idx = t.schema.indexOf(c).orElseThrow(()->new DBException("Unknown column "+c));
                projIdx.add(idx);
            }
        }
        List<String> headers = new ArrayList<>();
        for (int idx: projIdx) headers.add(t.schema.get(idx).name+"("+t.schema.get(idx).type+")");

        List<List<Object>> rows = new ArrayList<>();
        for (com.minidb.storage.Record r: heap.scan()){
            if (sel.where!=null && !evalBool(sel.where, t.schema, r)) continue;
            List<Object> out = new ArrayList<>();
            for (int idx: projIdx) out.add(r.values.get(idx));
            rows.add(out);
        }
        return Result.table(headers, rows);
    }

    // ---------- expression evaluation ----------
    private boolean evalBool(ast.Expr e, Schema schema, com.minidb.storage.Record r){
        if (e instanceof ast.And a) return evalBool(a.l, schema, r) && evalBool(a.r, schema, r);
        if (e instanceof ast.Or  o) return evalBool(o.l, schema, r) ||  evalBool(o.r, schema, r);
        if (e instanceof ast.Not n) return !evalBool(n.e, schema, r);
        if (e instanceof ast.IsNull iz) return eval(iz.e, schema, r)==null;
        if (e instanceof ast.IsNotNull inn) return eval(inn.e, schema, r)!=null;
        if (e instanceof ast.Like l){
            Object lv = eval(l.l, schema, r), rv = eval(l.r, schema, r);
            return String.valueOf(lv).contains(String.valueOf(rv));
        }
        if (e instanceof ast.Compare c){
            Object lv = eval(c.left, schema, r), rv = eval(c.right, schema, r);
            if (lv==null || rv==null){
                return switch(c.op){
                    case "=" -> Objects.equals(lv, rv);
                    case "!=" -> !Objects.equals(lv, rv);
                    default   -> false;
                };
            }
            int cmp;
            if (lv instanceof Number && rv instanceof Number)
                cmp = Integer.compare(((Number)lv).intValue(), ((Number)rv).intValue());
            else
                cmp = String.valueOf(lv).compareTo(String.valueOf(rv));
            return switch(c.op){
                case "=" -> cmp==0;
                case "<" -> cmp<0;
                case ">" -> cmp>0;
                case "<=" -> cmp<=0;
                case ">=" -> cmp>=0;
                case "!=" -> cmp!=0;
                default -> false;
            };
        }
        Object v = eval(e, schema, r);
        if (v instanceof Boolean b) return b;
        return v!=null;
    }

    private Object eval(ast.Expr e, Schema schema, com.minidb.storage.Record r){
        if (e instanceof ast.Literal lit) return lit.v;
        if (e instanceof ast.ColRef cr){
            int idx = schema.indexOf(cr.name).orElseThrow(()->new DBException("Unknown column "+cr.name));
            return r.values.get(idx);
        }
        throw new DBException("Bad expr");
    }

    private Object castTo(Column.Type type, Object v){
        if (v==null) return null;
        if (type==Column.Type.INT){
            if (v instanceof Number) return ((Number)v).intValue();
            try { return Integer.parseInt(String.valueOf(v)); }
            catch(Exception e){ throw new DBException("Cannot cast '"+v+"' to INT"); }
        }
        return String.valueOf(v);
    }
    
    private Result doCreateIndex(ast.CreateIndex ci) {
        try {
            indexManager.createIndex(ci.indexName, ci.tableName, ci.columnName);
            return Result.message("Index created: " + ci.indexName);
        } catch (Exception e) {
            return Result.error("Failed to create index: " + e.getMessage());
        }
    }
    
    private Result doDropIndex(ast.DropIndex di) {
        try {
            indexManager.dropIndex(di.indexName);
            return Result.message("Index dropped: " + di.indexName);
        } catch (Exception e) {
            return Result.error("Failed to drop index: " + e.getMessage());
        }
    }
    
    private Result doBeginTransaction(ast.BeginTransaction bt) {
        try {
            currentTransactionId = transactionManager.beginTransaction();
            return Result.message("Transaction started: " + currentTransactionId);
        } catch (Exception e) {
            return Result.error("Failed to begin transaction: " + e.getMessage());
        }
    }
    
    private Result doCommitTransaction(ast.CommitTransaction ct) {
        try {
            if (currentTransactionId == -1) {
                return Result.error("No active transaction");
            }
            transactionManager.commitTransaction(currentTransactionId);
            long tid = currentTransactionId;
            currentTransactionId = -1;
            return Result.message("Transaction committed: " + tid);
        } catch (Exception e) {
            return Result.error("Failed to commit transaction: " + e.getMessage());
        }
    }
    
    private Result doRollbackTransaction(ast.RollbackTransaction rt) {
        try {
            if (currentTransactionId == -1) {
                return Result.error("No active transaction");
            }
            transactionManager.rollbackTransaction(currentTransactionId);
            long tid = currentTransactionId;
            currentTransactionId = -1;
            return Result.message("Transaction rolled back: " + tid);
        } catch (Exception e) {
            return Result.error("Failed to rollback transaction: " + e.getMessage());
        }
    }


    // ---- Result ----
    public static class Result {
        public enum Kind { MESSAGE, TABLE, ERROR }
        public final Kind kind; public final String message;
        public final List<String> headers; public final List<List<Object>> rows;
        private Result(Kind k, String msg, List<String> h, List<List<Object>> r){ kind=k; message=msg; headers=h; rows=r; }
        public static Result message(String m){ return new Result(Kind.MESSAGE, m, List.of(), List.of()); }
        public static Result table(List<String> h, List<List<Object>> r){ return new Result(Kind.TABLE, null, h, r); }
        public static Result error(String m){ return new Result(Kind.ERROR, m, List.of(), List.of()); }
    }
}

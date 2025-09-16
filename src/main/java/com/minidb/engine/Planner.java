package com.minidb.engine;
import com.minidb.engine.plan.*;
import com.minidb.sql.ast;
public class Planner {
    public static LogicalPlan plan(ast.Select s){
        LogicalPlan p = new SeqScan(s.table);
        if (s.where != null) p = new Filter(p, s.where);
        p = new Project(p, s.cols);
        return p;
    }
}

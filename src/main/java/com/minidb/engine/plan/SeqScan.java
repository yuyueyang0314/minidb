package com.minidb.engine.plan;
public class SeqScan implements LogicalPlan {
    public final String table;
    public SeqScan(String table){ this.table = table; }
}

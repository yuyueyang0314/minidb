package com.minidb.engine.plan;
import com.minidb.sql.ast.Expr;
public class Filter implements LogicalPlan {
    public final LogicalPlan input; public final Expr predicate;
    public Filter(LogicalPlan in, Expr pred){ this.input=in; this.predicate=pred; }
}

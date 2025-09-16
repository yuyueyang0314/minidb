package com.minidb.engine.plan;

import com.minidb.sql.ast.Expr;
import com.minidb.sql.ast.JoinClause;

public class Join implements LogicalPlan {
    public final LogicalPlan left;
    public final LogicalPlan right;
    public final JoinClause.Type type;
    public final Expr condition;
    
    public Join(LogicalPlan left, LogicalPlan right, JoinClause.Type type, Expr condition) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.condition = condition;
    }
}

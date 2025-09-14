package com.minidb.engine.plan;
import java.util.*;
public class Project implements LogicalPlan {
    public final LogicalPlan input; public final List<String> cols;
    public Project(LogicalPlan in, List<String> cols){ this.input=in; this.cols=cols; }
}

package com.minidb.engine;
import com.minidb.engine.plan.*;
import com.minidb.sql.ast;
import java.util.*;

public class Planner {
    public static LogicalPlan plan(ast.Select s){
        LogicalPlan p = new SeqScan(s.table);
        
        // 处理JOIN操作
        for (var join : s.joins) {
            p = new Join(p, new SeqScan(join.table), join.type, join.condition);
        }
        
        if (s.where != null) p = new Filter(p, s.where);
        p = new Project(p, s.cols);
        return p;
    }
    
    public static String toTree(LogicalPlan plan) {
        return toTree(plan, 0);
    }
    
    private static String toTree(LogicalPlan plan, int indent) {
        String prefix = "  ".repeat(indent);
        if (plan instanceof SeqScan scan) {
            return prefix + "SeqScan(table=" + scan.table + ")";
        } else if (plan instanceof Filter filter) {
            return prefix + "Filter(condition=" + filter.predicate + ")\n" + 
                   toTree(filter.input, indent + 1);
        } else if (plan instanceof Project project) {
            return prefix + "Project(columns=" + project.cols + ")\n" + 
                   toTree(project.input, indent + 1);
        } else if (plan instanceof Join join) {
            return prefix + "Join(type=" + join.type + ", condition=" + join.condition + ")\n" +
                   toTree(join.left, indent + 1) + "\n" +
                   toTree(join.right, indent + 1);
        }
        return prefix + "Unknown(" + plan.getClass().getSimpleName() + ")";
    }
    
    public static String toJSON(LogicalPlan plan) {
        return toJSON(plan, 0);
    }
    
    private static String toJSON(LogicalPlan plan, int indent) {
        String prefix = "  ".repeat(indent);
        if (plan instanceof SeqScan scan) {
            return prefix + "{\n" +
                   prefix + "  \"type\": \"SeqScan\",\n" +
                   prefix + "  \"table\": \"" + scan.table + "\"\n" +
                   prefix + "}";
        } else if (plan instanceof Filter filter) {
            return prefix + "{\n" +
                   prefix + "  \"type\": \"Filter\",\n" +
                   prefix + "  \"condition\": \"" + filter.predicate + "\",\n" +
                   prefix + "  \"input\": " + toJSON(filter.input, indent + 1) + "\n" +
                   prefix + "}";
        } else if (plan instanceof Project project) {
            return prefix + "{\n" +
                   prefix + "  \"type\": \"Project\",\n" +
                   prefix + "  \"columns\": " + toJSONArray(project.cols) + ",\n" +
                   prefix + "  \"input\": " + toJSON(project.input, indent + 1) + "\n" +
                   prefix + "}";
        } else if (plan instanceof Join join) {
            return prefix + "{\n" +
                   prefix + "  \"type\": \"Join\",\n" +
                   prefix + "  \"joinType\": \"" + join.type + "\",\n" +
                   prefix + "  \"condition\": \"" + join.condition + "\",\n" +
                   prefix + "  \"left\": " + toJSON(join.left, indent + 1) + ",\n" +
                   prefix + "  \"right\": " + toJSON(join.right, indent + 1) + "\n" +
                   prefix + "}";
        }
        return prefix + "{\"type\": \"Unknown\"}";
    }
    
    private static String toJSONArray(List<String> list) {
        return "[" + String.join(", ", list.stream().map(s -> "\"" + s + "\"").toArray(String[]::new)) + "]";
    }
    
    public static String toSExpression(LogicalPlan plan) {
        if (plan instanceof SeqScan scan) {
            return "(SeqScan " + scan.table + ")";
        } else if (plan instanceof Filter filter) {
            return "(Filter " + filter.predicate + " " + toSExpression(filter.input) + ")";
        } else if (plan instanceof Project project) {
            return "(Project " + toSExpressionList(project.cols) + " " + toSExpression(project.input) + ")";
        } else if (plan instanceof Join join) {
            return "(Join " + join.type + " " + join.condition + " " + 
                   toSExpression(join.left) + " " + toSExpression(join.right) + ")";
        }
        return "(Unknown)";
    }
    
    private static String toSExpressionList(List<String> list) {
        return "(" + String.join(" ", list) + ")";
    }
}

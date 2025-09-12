package com.minidb.storage;
import java.util.*;
public class Record {
    public final List<Object> values;
    public Record(List<Object> values){ this.values = values; }
    public String toString(){ return values.toString(); }
}

package com.minidb.utils;
public class DBException extends RuntimeException {
    public DBException(String m){ super(m); }
    public DBException(String m, Throwable t){ super(m, t); }
}

package me.guozi.calcite.example.redis;


import java.util.LinkedList;
import java.util.List;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisData {

    public static class Table {
        public String tableName;
        public List<Column> columns = new LinkedList<>();
        public List<List<String>> data = new LinkedList<>();
    }

    public static class Column {
        public String name;
        public String type;
    }

    public static Column createColumn(String name, String type) {
        Column column = new Column();
        column.name = name;
        column.type = type;
        return column;
    }
}

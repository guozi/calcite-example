package me.guozi.calcite.example.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * @author chenyun
 * @date 2018/11/19
 */
public class DataBridge {
    /**
     * 静态成员变量，支持单态模式
     */
    private static DataBridge dataBridge = null;

    private DataBridge() {

    }

    /**
     * 单态模式获取实例
     *
     * @return DataBridge对象
     */
    public static synchronized DataBridge getInstance() {
        if (dataBridge == null) {
            dataBridge = new DataBridge();
        }
        return dataBridge;
    }

    public List<RedisData.Table> getTables() {
        List<RedisData.Table> tables = new LinkedList<>();
        RedisData.Table student = new RedisData.Table();
        student.tableName = "Student";
        RedisData.Column id = RedisData.createColumn("id", "long");
        RedisData.Column name = RedisData.createColumn("name", "string");
        RedisData.Column birthday = RedisData.createColumn("birthday", "date");
        RedisData.Column age = RedisData.createColumn("age", "int");
        student.columns.add(id);
        student.columns.add(name);
        student.columns.add(birthday);
        student.columns.add(age);
        tables.add(student);

        RedisData.Table classes = new RedisData.Table();
        classes.tableName = "Classes";
        RedisData.Column classesId = RedisData.createColumn("id", "long");
        RedisData.Column classesName = RedisData.createColumn("name", "string");
        RedisData.Column studentId = RedisData.createColumn("studentId", "long");
        classes.columns.add(classesId);
        classes.columns.add(classesName);
        classes.columns.add(studentId);
        tables.add(classes);

        return tables;
    }

    public List<List<String>> getTableData(String tableName) {
        List<List<String>> rows = new ArrayList<>();
//        if ("Student".equals(tableName)) {
//            rows.add(Arrays.asList("1", "Sam", "1990-10-11", "28"));
//            rows.add(Arrays.asList("2", "Tom", "1980-02-11", "38"));
//            rows.add(Arrays.asList("3", "Jan", "1989-11-23", "29"));
//            rows.add(Arrays.asList("4", "Tim", "", ""));
//        }
//        if ("Classes".equals(tableName)) {
//            rows.add(Arrays.asList("1", "classes-A", "1"));
//            rows.add(Arrays.asList("2", "classes-A", "2"));
//            rows.add(Arrays.asList("3", "classes-A", "4"));
//        }
        return rows;
    }
}

package me.guozi.calcite.example.redis;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisEnumerator<E> implements Enumerator<E> {

    private final List<List<String>> datas;
    private final RowConverter<E> rowConverter;
    private int currentIndex = -1;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    RedisEnumerator(List<RedisFieldType> fieldTypes, List<List<String>> datas) {
        this(fieldTypes, identityList(fieldTypes.size()), datas);
    }

    RedisEnumerator(List<RedisFieldType> fieldTypes, int[] fields, List<List<String>> datas) {
        this((RowConverter<E>) converter(fieldTypes, fields), datas);
    }

    RedisEnumerator(RowConverter<E> rowConverter, List<List<String>> datas) {
        this.rowConverter = rowConverter;
        this.datas = datas;
    }

    private static RowConverter<?> converter(List<RedisFieldType> fieldTypes, int[] fields) {
        if (fields.length == 1) {
            final int field = fields[0];
            return new SingleColumnRowConverter(fieldTypes.get(field), field);
        } else {
            return new ArrayRowConverter(fieldTypes, fields);
        }
    }

    static RelDataType deduceRowType(JavaTypeFactory typeFactory, RedisData.Table table, List<RedisFieldType> fieldTypes) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        List<RedisData.Column> columns = table.columns;
        for (RedisData.Column column : columns) {
            final String name = column.name;
            final RedisFieldType fieldType = RedisFieldType.of(column.type);

            if (fieldType == null) {
                System.out.println("WARNING: Found unknown type: "
                    + column.type + " in table: " + table.tableName
                    + " for column: " + name
                    + ". Will assume the type of column is string");
            }
            final RelDataType type;
            if (fieldType == null) {
                type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            } else {
                type = fieldType.toType(typeFactory);
            }
            names.add(name);
            types.add(type);

            if (fieldTypes != null) {
                fieldTypes.add(fieldType);
            }
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }


    @Override
    public E current() {
        List<String> rows = datas.get(currentIndex);
        return rowConverter.convertRow(rows.toArray(new String[0]));
    }

    @Override
    public boolean moveNext() {
        return ++currentIndex < datas.size();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {
        abstract E convertRow(String[] rows);

        protected Object convert(RedisFieldType fieldType, String string) {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
                case BOOLEAN:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case BYTE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Byte.parseByte(string);
                case SHORT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Short.parseShort(string);
                case INT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
                case LONG:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Long.parseLong(string);
                case FLOAT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Float.parseFloat(string);
                case DOUBLE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Double.parseDouble(string);
                case DATE:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return (int) date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter<Object[]> {
        private final RedisFieldType[] fieldTypes;
        private final int[] fields;

        ArrayRowConverter(List<RedisFieldType> fieldTypes, int[] fields) {
            this.fieldTypes = fieldTypes.toArray(new RedisFieldType[0]);
            this.fields = fields;
        }

        @Override
        public Object[] convertRow(String[] strings) {
            return convertNormalRow(strings);
        }

        private Object[] convertNormalRow(String[] strings) {
            final Object[] objects = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                objects[i] = convert(fieldTypes[field], strings[field]);
            }
            return objects;
        }
    }

    /**
     * Single column row converter.
     */
    static class SingleColumnRowConverter extends RowConverter {
        private final RedisFieldType fieldType;
        private final int fieldIndex;

        private SingleColumnRowConverter(RedisFieldType fieldType, int fieldIndex) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Object convertRow(String[] strings) {
            return convert(fieldType, strings[fieldIndex]);
        }
    }
}

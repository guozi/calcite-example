package me.guozi.calcite.example.redis;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenyun
 */

public enum RedisFieldType {

    //自定义类型
    VARCHAR(String.class, "varchar"),
    DECIMAL(BigDecimal.class, "decimal"),
    BIGINT(Long.class, "bigint"),

    //primitive 类型
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),

    //class 类型
    STRING(String.class, "string"),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, RedisFieldType> MAP = new HashMap<>();

    static {
        for (RedisFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    RedisFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
    }

    RedisFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        //JavaToSqlTypeConversionRules 支持转换的类型
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

    public static RedisFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}

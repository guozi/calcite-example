package me.guozi.calcite.example.redis;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisSchemaFactory implements SchemaFactory {
    /**
     * 建立Connection的时候，即通过读取定义的元数据文件，获取指定的SchemaFatory
     * 然后通过这个SchemaFactory来创建Schema。
     */
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        System.out.println("param1 : " + operand.get("param1"));
        System.out.println("param2 : " + operand.get("param2"));
        System.out.println("Get database " + name);
        return new RedisSchema(DataBridge.getInstance().getTables());
    }
}

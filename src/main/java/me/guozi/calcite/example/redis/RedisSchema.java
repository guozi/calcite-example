package me.guozi.calcite.example.redis;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import me.guozi.calcite.example.redis.function.UdfOperator;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.List;
import java.util.Map;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisSchema extends AbstractSchema {

    private final List<RedisData.Table> redisTables;
    private Map<String, Table> tableMap;

    public RedisSchema(List<RedisData.Table> redisTables) {
        this.redisTables = redisTables;
    }

    /**
     * 通过schema实现从数据源自己的概念（DataBase及Table）向Calcite的概念(MemoryTable)进行转换的过程。
     */
    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        // Build a map from table name to table; each file becomes a table.
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (RedisData.Table redisTable : redisTables) {
            builder.put(redisTable.tableName, new RedisScannableTable(redisTable, null));
        }
        return builder.build();
    }


    /**
     * 获取操作函数中所有操作函数，将这些函数转换为Calcite中的概念。
     *
     * @return
     */
    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        Multimap<String, Function> functions = HashMultimap.create();

        ImmutableMultimap<String, ScalarFunction> funcs = ScalarFunctionImpl.createAll(UdfOperator.class);
        for (String key : funcs.keySet()) {
            for (ScalarFunction func : funcs.get(key)) {
                functions.put(key, func);
            }
        }

        return functions;
    }
}

package me.guozi.calcite.example.redis;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisTable extends AbstractTable {
    protected final RedisData.Table table;
    protected final RelProtoDataType protoRowType;

    protected List<RedisFieldType> fieldTypes;

    public RedisTable(RedisData.Table table, RelProtoDataType protoRowType) {
        this.table = table;
        this.protoRowType = protoRowType;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        if (fieldTypes == null) {
            fieldTypes = new ArrayList<>();
            return RedisEnumerator.deduceRowType((JavaTypeFactory) typeFactory, table, fieldTypes);
        } else {
            return RedisEnumerator.deduceRowType((JavaTypeFactory) typeFactory, table, null);
        }
    }
}

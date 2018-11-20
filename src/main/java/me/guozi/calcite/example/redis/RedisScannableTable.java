package me.guozi.calcite.example.redis;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenyun
 * @date 2018/11/19
 */
public class RedisScannableTable extends RedisTable implements ScannableTable {

    public RedisScannableTable(RedisData.Table table, RelProtoDataType protoRowType) {
        super(table, protoRowType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        final int[] fields = RedisEnumerator.identityList(fieldTypes.size());

        CalciteConnection conn = (CalciteConnection) root.getQueryProvider();

        String tableName = conn.getProperties().getProperty("table_name");

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new RedisEnumerator<>(new RedisEnumerator.ArrayRowConverter(fieldTypes, fields),
                    DataBridge.getInstance().getTableData(tableName));
            }
        };
    }
}

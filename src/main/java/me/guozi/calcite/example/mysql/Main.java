package me.guozi.calcite.example.mysql;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by chenyun on 2019-03-15.
 */
public class Main {

    public static void main(String[] args) {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");

        try {
            //MySQL
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl("jdbc:mysql://localhost/hr");
            dataSource.setUsername("root");
            dataSource.setPassword("123456");

            //Oracle
            BasicDataSource oracleDataSource = new BasicDataSource();
            oracleDataSource.setDriverClassName("oracle.jdbc.OracleDriver");
            oracleDataSource.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:hr");
            oracleDataSource.setUsername("root");
            oracleDataSource.setPassword("123456");

            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Schema creditSchema = JdbcSchema.create(rootSchema, "credit", dataSource, null, null);
            Schema approveSchema = JdbcSchema.create(rootSchema, "approve", oracleDataSource, null, "NEWAPP");

            rootSchema.add("credit", creditSchema);
            rootSchema.add("approve", approveSchema);

            String creditSql = "select vcr_id, `result` from credit.variable_calculate_result  where vcr_id = 1132";
            String approveSql = "SELECT NODE from approve.DA_FILTRATE_OUT where APPLY_CODE = 'A20180912103000694' and STATUS = 'VALID'";
            String mixedSql = "select count(1) from credit.idx_calc_biz_req a, approve.APPLY_INFO b where a.data_no = b.APPLY_CODE and a.data_no = 'A20180912103000694'";


            Statement statement = calciteConnection.createStatement();

            ResultSet resultSet = statement.executeQuery(creditSql);
            while (resultSet.next()) {
                System.out.println("vcrId : " + resultSet.getString(1) + ", Result : " + resultSet.getString(2));
            }
            resultSet.close();

//            ResultSet resultSet1 = statement.executeQuery(approveSql);
//            while (resultSet1.next()) {
//                System.out.println("node : " + resultSet1.getString(1));
//            }
//            resultSet1.close();
//
//            long start1 = System.currentTimeMillis();
//            ResultSet resultSet2 = statement.executeQuery(mixedSql);
//            while (resultSet2.next()) {
//                System.out.println("params : " + resultSet2.getString(1));
//            }
//            resultSet2.close();
//            System.out.println(System.currentTimeMillis() - start1);

            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}


package com.zhu.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/22  18:00
 */
public class FlinkCdcBySql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, environmentSettings);


        tableEnvironment.executeSql("CREATE TABLE products (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  description STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'hadoop1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'database-name' = 'flink_cdc',\n" +
                "  'table-name' = 'products'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE orders (\n" +
                "  order_id INT,\n" +
                "  order_date TIMESTAMP(0),\n" +
                "  customer_name STRING,\n" +
                "  price DECIMAL(10, 5),\n" +
                "  product_id INT,\n" +
                "  order_status BOOLEAN\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'hadoop1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'database-name' = 'flink_cdc',\n" +
                "  'table-name' = 'orders'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE shipments (\n" +
                "  shipment_id INT,\n" +
                "  order_id INT,\n" +
                "  origin STRING,\n" +
                "  destination STRING,\n" +
                "  is_arrived BOOLEAN\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'hadoop1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root',\n" +
                "  'database-name' = 'flink_cdc',\n" +
                "  'table-name' = 'shipments'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE enriched_orders (\n" +
                "  order_id INT,\n" +
                "  order_date TIMESTAMP(0),\n" +
                "  customer_name STRING,\n" +
                "  price DECIMAL(10, 5),\n" +
                "  product_id INT,\n" +
                "  order_status BOOLEAN,\n" +
                "  product_name STRING,\n" +
                "  product_description STRING,\n" +
                "  shipment_id INT,\n" +
                "  origin STRING,\n" +
                "  destination STRING,\n" +
                "  is_arrived BOOLEAN,\n" +
                "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'hosts' = '',\n" +
                "    'topic' = 'enriched_orders'\n" +
                ")");

        tableEnvironment.executeSql("INSERT INTO enriched_orders\n" +
                "SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived\n" +
                "FROM orders AS o\n" +
                "LEFT JOIN products AS p ON o.product_id = p.id\n" +
                "LEFT JOIN shipments AS s ON o.order_id = s.order_id");

        Table productTable = tableEnvironment.sqlQuery("select * from products");
        tableEnvironment.toRetractStream(productTable, Row.class).print("Product");

        Table orderTable = tableEnvironment.sqlQuery("select * from orders");
        tableEnvironment.toRetractStream(orderTable, Row.class).print("Order");

        Table shipmentTable = tableEnvironment.sqlQuery("select * from shipments");
        tableEnvironment.toRetractStream(shipmentTable, Row.class).print("Shipments");

        environment.execute();
    }
}

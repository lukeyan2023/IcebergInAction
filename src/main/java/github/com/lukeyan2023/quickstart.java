package github.com.lukeyan2023;

import org.apache.hadoop.hive.serde2.TypedSerDe;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class quickstart {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Apache Iceberg API")
                .master("local[1]")
                .getOrCreate();
        CreateTableUsingHiveCatalog(spark);
    }
    public static void CreateTableUsingHiveCatalog(SparkSession spark) {
        // create catalog
        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(spark.sparkContext().hadoopConfiguration());

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", "hdfs://namenode1/iceberg/warehouse");
        properties.put("uri", "thrift://cnguls032:9083,thrift://cnguls033:9083,thrift://cnguls034:9083");

        catalog.initialize("hive", properties);

        // create schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        // create partition
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        TableIdentifier name = TableIdentifier.of("logging", "logs");

        Table table = catalog.createTable(name, schema, spec);
    }
}

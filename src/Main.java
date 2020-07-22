import com.google.cloud.bigquery.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main  {
    public static void main(String[] args)  throws InterruptedException, SQLException {
        MigrateData migrateData = new MigrateData();
        migrateData.startMigrateData();
    }

    public static Dataset createDataset(BigQuery bigquery,String query){
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(query).build();
        Dataset newDataset = bigquery.create(datasetInfo);
        return newDataset;
    }

    public static List<Table> createTables(Dataset dataset, ResultSet tableResultset, DataTypeConversion dataTypeConversion) throws SQLException {
        List<Table> tableList = new ArrayList<Table>();
        List<Field> fieldList = new ArrayList<Field>();
        Map<String, String> fieldTypeMap = new HashMap<>();
        String tableName = "";
        String fieldName = "";
        String fieldType = "";

        while (tableResultset.next()) {
            fieldName = tableResultset.getString("field_name");
            fieldType = tableResultset.getString("field_type");
            fieldType = dataTypeConversion.parser(fieldType);

            if (!tableResultset.getString("field_number").equals(tableResultset.getString("number_of_fields"))) {
                fieldTypeMap.put(fieldName, fieldType);
            } else {
                tableName = tableResultset.getString("table_name");
                fieldTypeMap.put(fieldName, fieldType);
                fieldList = createFields(fieldTypeMap);
                Schema schema = Schema.of(fieldList);
                fieldTypeMap.clear();
                fieldList.clear();
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                tableList.add(dataset.create(tableName, tableDefinition));
            }
        }

        return tableList;
    }

    public static List<Field> createFields(Map<String,String> fieldTypeMap){
        List<Field> fieldList = new ArrayList<Field>();
        for ( String key :  fieldTypeMap.keySet() ){
            String type = fieldTypeMap.get(key);
            Field field = Field.newBuilder(key,StandardSQLTypeName.valueOf(type)).build();
            fieldList.add(field);
        }
        return fieldList;
    }
}

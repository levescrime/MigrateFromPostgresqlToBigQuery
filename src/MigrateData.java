import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrateData {
    Query query;
    String host = "jdbc:postgresql://localhost:5432/";
    String userName = "postgres";
    String password = "q1w2e3r4";
    String projectId = "apeakdata";
    String path = "C:\\Users\\levav\\Documents\\data\\%s.csv";
    File credentialsPath = new File("C:\\MyFolder\\ApeakData-4a7afc84ec8f.json");
    DBConnect dbConnect = new DBConnect(host, userName, password);
    GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
    DataTypeConversion dataTypeConversion = new DataTypeConversion();
    List<Table> tableList = new ArrayList<Table>();

    public MigrateData(){
        this.query = new Query();
    }

    public void startMigrateData() throws SQLException {
        dbConnect.Connect();
        //connecting to BQ
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BigQuery bigquery =
                BigQueryOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(projectId)
                        .build()
                        .getService();
        //End of connecting

        ResultSet schemasResultset = dbConnect.executeQuery(query.getQueryByName("getSchemas"));

        while (schemasResultset.next()) {
            String datasetName = schemasResultset.getString(1);
            Dataset dataset = createDataset(bigquery, datasetName);
            //creating tables
            String getTablesQuery = query.getQueryByName("getTables");
            String queryForTables = String.format(getTablesQuery, datasetName);
            dbConnect.Reconnect();
            ResultSet tablesResultset = dbConnect.executeQuery(queryForTables);
            createTables(dataset, tablesResultset, dataTypeConversion);
            tablesResultset.beforeFirst();
            copyDataFromSource(tablesResultset, datasetName);
            tablesResultset.beforeFirst();
            loadIntoTarget(tablesResultset,datasetName);
            dbConnect.Reconnect();
//            loadDataFromTable();
        }

        for (Table table : tableList) {
            String fullTableName = table.getTableId().getDataset() + "." + table.getTableId().getTable();
            dbConnect.Reconnect();
            String tableQuery = query.getQueryByName("select");
            tableQuery = String.format(tableQuery, fullTableName);
            ResultSet select = dbConnect.executeQuery(tableQuery);
        }
    }

    public void copyDataFromSource(ResultSet resultSet, String datasetName){

    }

    public void loadIntoTarget(ResultSet resultSet, String datasetName) throws SQLException {
        while(resultSet.next()){
            if (resultSet.getString("field_number").equals(resultSet.getString("number_of_fields"))) {
                String tableName = resultSet.getString(1);
                TableId tableId = TableId.of(datasetName,tableName);
            }
        }
    }


//    public void writeToFile(ResultSet resultSet, String fileName) throws IOException, SQLException {
//        path = String.format(path,fileName);
//        File dataFile = new File(path);
//        FileWriter fstream = new FileWriter(dataFile);
//        BufferedWriter out = new BufferedWriter(fstream);
//
////        while (resultSet.next()) {
////            out.write(resultSet.getString(1) + ",");
////            out.write(select.getString(2) + ",");
////            out.write(select.getString(3) + ",");
////            out.write(select.getString(4));
////            out.newLine();
////            /*out.write(System.getProperty("line.separator"));*/
////        }
//        System.out.println("Completed writing into text file");
//        out.close();
//    }

    public static void loadData(String fullTableName, String csvPath, String schema) throws IOException {
        String command = "cmd /c" + fullTableName + " " + csvPath + " " + schema;
        Process process = Runtime.getRuntime().exec(command);
    }


    public static Dataset createDataset(BigQuery bigquery, String datasetName){
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
        Dataset newDataset = bigquery.create(datasetInfo);
        return newDataset;
    }

    public  List<Table> createTables(Dataset dataset, ResultSet tableResultset, DataTypeConversion dataTypeConversion) throws SQLException {
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

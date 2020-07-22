import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StartTransferToBigQuery {
    DBConnect m_DBconnect;
    DataTypeConversion m_DataTypeConverter;
    BigQuery m_BigQuery ;
    private boolean isDatasetCreated;
    Dataset newDataset;
    Schema schema;
    Table table;


    StartTransferToBigQuery(DBConnect i_DBconnect){
        this.m_DBconnect = i_DBconnect;
        this.m_DataTypeConverter = new DataTypeConversion();
        this.isDatasetCreated = false;
        connectToDataBases();
    }

    private void connectToDataBases(){
        this.m_DBconnect.Connect();
    }


// get tables from postgress
    public  ResultSet CreateAndParseTablesInBigQuery(String i_Query){
        try{
            ResultSet query_resultSet = this.m_DBconnect.executeQuery(i_Query);
            //String query =  parser(query_resultSet);
            //CreateAllTables(query);
            return query_resultSet;
        } catch (
                SQLException err) {
            System.out.println(err.getMessage());
            return null;
        }
    }

//    public String parser(ResultSet i_ResultSet) throws SQLException {
//        String stringAfterParsing = "";
//        String stringBeforeParsing ;
//        Set set =  m_DataTypeConverter.m_RegexMap.entrySet();
//        //iterating over all the create tables and change the Data Type accordingly
//        while (i_ResultSet.next()){
//            stringBeforeParsing = i_ResultSet.getString("ddl_st");
//            Iterator itr = set.iterator();
//            while (itr.hasNext()){
//                Map.Entry entry = (Map.Entry)itr.next();
//                stringBeforeParsing = stringBeforeParsing.replaceAll(entry.getKey().toString(),entry.getValue().toString());
//            }
//            System.out.println("string in parsing = " + stringBeforeParsing);
//            CreateAllTables(stringBeforeParsing);
//            stringAfterParsing += stringBeforeParsing;
//        }
//        return stringAfterParsing;
//    }
//
//    public void CreateAllTables(String i_Resultset) throws SQLException {
//        String projectId = "apeakdata";
//        File credentialsPath = new File("C:\\MyFolder\\ApeakData-4a7afc84ec8f.json");
//        GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
//        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
//            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        this.m_BigQuery =
//                BigQueryOptions.newBuilder()
//                        .setCredentials(credentials)
//                        .setProjectId(projectId)
//                        .build()
//                        .getService();
//        String[] Names;
//        String tableName = "";
//        String datasetName = "";
//            if (this.isDatasetCreated == false){
//                System.out.println("IF");
//                Names = CreateDataset(i_Resultset);
//                datasetName = Names[0];
//                tableName = Names[1];
//                this.isDatasetCreated = true;
//            }
//        System.out.println("result set  = " + i_Resultset);
//            CreateTable(i_Resultset,datasetName,tableName);
//    }
//
//    public String[] CreateDataset(String i_query){
//        System.out.println("created dataset");
//        String[] namesArr = findDatasetName(i_query);
//        DatasetInfo datasetInfo = DatasetInfo.newBuilder(namesArr[0]).build();
//        this.newDataset = this.m_BigQuery.create(datasetInfo);
//        System.out.println("created dataset");
//        return namesArr;
//    }
//
//
//    public void CreateTable(String i_query, String i_datasetName, String i_tableName){
//        Map fieldTypeMap = findFieldNamesAndTypes(i_query);
//        Field field;
//        for ( Object k :  fieldTypeMap.keySet() ) {
//            System.out.println("Key  = " + k + "v = "+ ((String) fieldTypeMap.get(k)));
//            switch ((String)fieldTypeMap.get(k)) {
//                case "int64":
//                    field = Field.of( (String)k, LegacySQLTypeName.INTEGER);
//                    System.out.println("Switch Entered");
//                    break;
//                case "bool":
//                    field = Field.of( (String)k, LegacySQLTypeName.BOOLEAN);
//                    System.out.println("Switch Entered");
//                    break;
//                case "float64":
//                    field = Field.of( (String)k, LegacySQLTypeName.FLOAT);
//                    System.out.println("Switch Entered");
//                    break;
//                case "numeric":
//                    field = Field.of( (String)k, LegacySQLTypeName.NUMERIC);
//                    System.out.println("Switch Entered");
//                    break;
//                case "date":
//                    field = Field.of( (String)k, LegacySQLTypeName.DATE);
//                    System.out.println("Switch Entered");
//                    break;
//                case "time":
//                    field = Field.of( (String)k, LegacySQLTypeName.TIME);
//                    System.out.println("Switch Entered");
//                    break;
//                case "timestamp":
//                    field = Field.of( (String)k, LegacySQLTypeName.TIMESTAMP);
//                    System.out.println("Switch Entered");
//                    break;
//                default:
//                    System.out.println("Switch Entered");
//                    field = Field.of( (String)k, LegacySQLTypeName.STRING);
//            }
//            this.schema = Schema.of(field);
//            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
//            TableId tableId = TableId.of(i_datasetName,i_tableName);
//            TableInfo  tableInfo = TableInfo.newBuilder(tableId,tableDefinition).build();
//            Table table = this.m_BigQuery.create(tableInfo);
//            table.update()
//        }
//    }
//
//    public static String[] findDatasetName(String query){
//        String res = query.substring("CREATE TABLE ".length(),query.indexOf("("));
//        String[] ans = res.split("\\.");
//        return ans;
//    }
//
//    public static Map findFieldNamesAndTypes(String query){
//        String res = query.substring(query.indexOf("(") + 1,query.indexOf(")"));
//        System.out.println("Res  = " + res);
//        String[] ans = res.split(",");
//        Map finalDataMap = new HashMap();
//        for (String str : ans){
//            String[] finalData = str.split(" ");
//            System.out.println("Map put  = " + finalData[0] + " Map v = " + finalData[1]);
//            finalDataMap.put(finalData[0],finalData[1]);
//        }
//        return finalDataMap;
//    }
}

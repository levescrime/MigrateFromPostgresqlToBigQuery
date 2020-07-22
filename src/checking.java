import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class checking {
    public static void main(String[] args) throws SQLException, InterruptedException, IOException {
        String GetAllTables =
                "SELECT                                          \n" +
                        "  'CREATE TABLE ' || nspname||'.'|| relname || E'\\n(\\n' ||\n" +
                        "  array_to_string(\n" +
                        "    array_agg(\n" +
                        "      '    ' || column_name || ' ' ||  type || ' '|| not_null\n" +
                        "    )\n" +
                        "    , E',\\n'\n" +
                        "  ) || E'\\n);\\n' as ddl_st\n" +
                        "from\n" +
                        "(\n" +
                        "  SELECT \n" +
                        "    c.relname, a.attname AS column_name, pn.nspname as nspname ,\n" +
                        "    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,\n" +
                        "    case \n" +
                        "      when a.attnotnull\n" +
                        "    then 'NOT NULL' \n" +
                        "    else 'NULL' \n" +
                        "    END as not_null \n" +
                        "  FROM pg_class c,\n" +
                        "   pg_attribute a,\n" +
                        "   pg_type t, \n" +
                        "   pg_catalog.pg_namespace pn\n" +
                        "   \n" +
                        "   WHERE 1=1\n" +
                        "   and pn.oid=c.relnamespace \n" +
                        "   AND a.attnum > 0\n" +
                        "   AND a.attrelid = c.oid\n" +
                        "   AND a.atttypid = t.oid\n" +
                        "   and pn.nspname not like 'pg%' and pn.nspname not in ('information_schema')\n" +
                        " ORDER BY a.attnum\n" +
                        ") as tabledefinition\n" +
                        "group by relname, nspname;";

        String host = "jdbc:postgresql://localhost:5432/";
        String userName = "postgres";
        String password = "q1w2e3r4";
        String projectId = "apeakdata";
        //String datasetName = "public";
        //String tableName = "LEV_DID_IT";
        //String fieldName = "HOW";
        DataTypeConversion dataTypeConversion = new DataTypeConversion();
        List<Dataset> datasetList = new ArrayList<Dataset>();
        File credentialsPath = new File("C:\\MyFolder\\ApeakData-4a7afc84ec8f.json");
        DBConnect dbConnect = new DBConnect(host, userName, password);
        GoogleCredentials credentials = GoogleCredentials.newBuilder().build();

        StartTransferToBigQuery startTransferToBigQuery = new StartTransferToBigQuery(dbConnect);
        //connectiong to BQ
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
        ResultSet resultSet = startTransferToBigQuery.CreateAndParseTablesInBigQuery(GetAllTables);
//        while (resultSet.next()){
//            String filtered = filter(resultSet.getString("ddl_st"));
//            //System.out.println("filtered = " + filtered);
//            String query = getDatasetName(filtered);
//            //System.out.println("dataswt name = " + query);
//            datasetList.add(createDataset(bigquery,query));
//        }
        String filtered = dataTypeConversion.parser("");
        System.out.println("Filtered = " + filtered);
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(filtered).setUseLegacySql(false).build();
        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build());
    }

}



// Sample to load CSV data from Cloud Storage into a new BigQuery table



//    QueryJobConfiguration queryConfig =
//            QueryJobConfiguration.newBuilder(
//                    "SELECT "
//                            + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
//                            + "view_count "
//                            + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
//                            + "WHERE tags like '%google-bigquery%' "
//                            + "ORDER BY favorite_count DESC LIMIT 10")
//                    // Use standard SQL syntax for queries.
//                    // See: https://cloud.google.com/bigquery/sql-reference/
//                    .setUseLegacySql(false)
//                    .build();
//
//    // Create a job ID so that we can safely retry.
//    JobId jobId = JobId.of(UUID.randomUUID().toString());
//    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//
//// Wait for the query to complete.
//    queryJob = queryJob.waitFor();

//                public static String getDatasetName (String query){
//                    System.out.println("indexOf . = " + query.indexOf("."));
//                    String res = query.substring("CREATE TABLE ".length(), query.indexOf("."));
//                    System.out.println("get dataset name = " + res);
//                    return res;
//                }
//                public static Dataset createDataset (BigQuery bigquery, String query){
//                    DatasetInfo datasetInfo = DatasetInfo.newBuilder(query).build();
//                    Dataset newDataset = bigquery.create(datasetInfo);
//                    return newDataset;
//                }
//                public static String filter (String query){
//                    String filtered = query.replaceAll("\\n", "");
//                    filtered = filtered.replaceAll("\\s\\s+", "");
//                    filtered = filtered.replaceAll(" NULL", "");
//                    return filtered;
//                }
//    //TableId tableId = TableId.of(tableNames[0],tableNames[1]);
                //TableInfo tableInfo = TableInfo.newBuilder(tableId,tableDefinition).build();
                //Table table = bigquery.create(tableInfo);
                //        System.out.println("Datasets:");
//        for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
//            System.out.printf("%s%n", dataset.getDatasetId().getDataset());
//    }

//    String GetAllTables =
//            "SELECT                                          \n" +
//                    "  'CREATE TABLE ' || nspname||'.'|| relname || E'\\n(\\n' ||\n" +
//                    "  array_to_string(\n" +
//                    "    array_agg(\n" +
//                    "      '    ' || column_name || ' ' ||  type || ' '|| not_null\n" +
//                    "    )\n" +
//                    "    , E',\\n'\n" +
//                    "  ) || E'\\n);\\n' as ddl_st\n" +
//                    "from\n" +
//                    "(\n" +
//                    "  SELECT \n" +
//                    "    c.relname, a.attname AS column_name, pn.nspname as nspname ,\n" +
//                    "    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,\n" +
//                    "    case \n" +
//                    "      when a.attnotnull\n" +
//                    "    then 'NOT NULL' \n" +
//                    "    else 'NULL' \n" +
//                    "    END as not_null \n" +
//                    "  FROM pg_class c,\n" +
//                    "   pg_attribute a,\n" +
//                    "   pg_type t, \n" +
//                    "   pg_catalog.pg_namespace pn\n" +
//                    "   \n" +
//                    "   WHERE 1=1\n" +
//                    "   and pn.oid=c.relnamespace \n" +
//                    "   AND a.attnum > 0\n" +
//                    "   AND a.attrelid = c.oid\n" +
//                    "   AND a.atttypid = t.oid\n" +
//                    "   and pn.nspname not like 'pg%' and pn.nspname not in ('information_schema')\n" +
//                    " ORDER BY a.attnum\n" +
//                    ") as tabledefinition\n" +
//                    "group by relname, nspname;";


//                switch (fieldTypeMap.get(key)) {
//        case "int64":
//            field = Field.newBuilder(key,StandardSQLTypeName.INT64).build();
//            break;
//        case "bool":
//            field = Field.newBuilder(key,StandardSQLTypeName.BOOL).build();
//            break;
//        case "float64":
//            field = Field.newBuilder(key,StandardSQLTypeName.FLOAT64).build();
//            break;
//        case "numeric":
//            field = Field.newBuilder(key,StandardSQLTypeName.NUMERIC).build();
//            break;
//        case "date":
//            field = Field.newBuilder(key,StandardSQLTypeName.DATE).build();
//            break;
//        case "time":
//            field = Field.newBuilder(key,StandardSQLTypeName.TIME).build();
//            break;
//        case "timestamp":
//            field = Field.newBuilder(key,StandardSQLTypeName.TIMESTAMP).build();
//            break;
//        default:
//            field = Field.newBuilder(key,StandardSQLTypeName.STRING).build();
//    }

//    Map rows = new HashMap<String,Object>();
//        rows.put("Lev",2026);
//    Set set = rows.entrySet();
//    Iterator itr = set.iterator();
//    InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableList.get(0));
//        builder.addRow(rows);
                //tableList.get(0).;
                //create Tables
//        while (resultSet.next()){
//            String filtered = filter(resultSet.getString("ddl_st"));
//            System.out.println("create tables = " + filtered);
//            String[] tableNames = getTableNames(filtered);
//            System.out.println("got table names = " + tableNames[0] + "AND = " + tableNames[1]);
//
//            List<Field> fieldList = getFields(bigquery,resultSet);
//            Schema schema  = Schema.of(fieldList);
//           // FieldValueList fieldValueList = fieldList;
//            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
//            //TableId tableId = TableId.of(tableNames[0],tableNames[1]);
//            //TableInfo tableInfo = TableInfo.newBuilder(tableId,tableDefinition).build();
//            //Table table = bigquery.create(tableInfo);
//            datasetList.get(0).create(tableNames[0],tableDefinition);
//            //InsertAllRequest.RowToInsert.
//            //table.insert(fieldList);
//        }
//            Field field =Field.of( fieldName, LegacySQLTypeName.STRING);
//            Schema schema = Schema.of();
//            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
//            //DatasetId datasetId = DatasetId.
//            TableId tableId = TableId.of(datasetName,tableName);
//            TableInfo tableInfo = TableInfo.newBuilder(tableId,tableDefinition).build();
//            Table table = bigquery.create(tableInfo);
//        String script = "";

//
//    public static String getTableName(String query){
//        System.out.println("query before = "  + query);
//        String str = query.substring("CREATE TABLE ".length(),query.indexOf("("));
//        System.out.println("query After = "  + query);
//        return str;
//    }


//    public static List<Field> getFields(String filtered) {
//        Map fieldTypeMap = new HashMap<String,String>();
//            filtered = filtered.substring(filtered.indexOf("(") + 1,filtered.indexOf(")"));
//            //filtered = filtered.replaceAll()
//            String[] fieldTypeArr = filtered.split(",");
//            for(String str : fieldTypeArr){
//                //System.out.println("field = " + str);
//                String[] fieldType = str.split(" ");
//                fieldTypeMap.put(fieldType[0],fieldType[1]);
//                //System.out.println("LIST OF FIELDS = " + fieldType[0] + " and = " + fieldType[1]);
//            }
//
//        return  createFields(fieldTypeMap);
//    }

//    public static List<Field> getFields(String fieldName, String fieldType) {
//        Map fieldTypeMap = new HashMap<String,String>();
//        fieldTypeMap
//
//        return  createFields(fieldTypeMap);


//    public static String filter(String query){
//        String filtered = query.replaceAll("\\n","");
//        filtered = filtered.replaceAll("\\s\\s+","");
//        filtered = filtered.replaceAll(" NULL","");
//        return filtered;
//    }

                //    public static Table createTables(Dataset dataset, String tableInfo){
//        String tableName = getTableName(tableInfo);
//        List<Field> fieldList = getFields(tableInfo);
//        Schema schema  = Schema.of(fieldList);
//        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
//
//        return dataset.create(tableName, tableDefinition);
//    }

                //            while (tablesResultset.next()){
//                //String tableInfo = filter(tablesResultset.getString("ddl_st"));
//                //tableInfo = dataTypeConversion.parser(tableInfo);
//                //String tableInfo = tablesResultset
//                createTables(dataset,tableInfo);
//            }

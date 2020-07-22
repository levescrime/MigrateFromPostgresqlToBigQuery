import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;

public class explicit {
    public static void main(String... args) throws Exception {
        Query query = new Query();
        String host = "jdbc:postgresql://localhost:5432/";
        String userName = "postgres";
        String password = "q1w2e3r4";
        String projectId = "apeakdata";
        File credentialsPath = new File("C:\\MyFolder\\ApeakData-4a7afc84ec8f.json");
        DBConnect dbConnect = new DBConnect(host, userName, password);
        GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
        DataTypeConversion dataTypeConversion = new DataTypeConversion();

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

        ResultSet select = dbConnect.executeQuery(query.getQueryByName("getSchemas"));
        Query newQuery = new Query();
        query.blabla(select);
        select.beforeFirst();
        newQuery.again(select);
        // Values of the row to insert
//        String location = "C:\\Users\\levav\\Documents\\data\\dataFile.csv";
//        File dataFile = new File(location);
//        FileWriter fstream = new FileWriter(dataFile);
//        BufferedWriter out = new BufferedWriter(fstream);
//        BufferedReader reader = new BufferedReader(new FileReader(location));
//        FileReader fileReader = new FileReader(location);
//        Path path = Paths.get(location);
//        while (select.next()) {
//            out.write(select.getString(1) + ",");
//            out.write(select.getString(2) + ",");
//            out.write(select.getString(3) + ",");
//            out.write(select.getString(4));
//            out.newLine();
//            /*out.write(System.getProperty("line.separator"));*/
//        }
//        System.out.println("Completed writing into text file");
//        out.close();
        TableId tableId = TableId.of("public", "accounts");
        System.out.println("tableId = " + tableId.getTable() + " dataset = " + tableId.getDataset());

//        ProcessBuilder pb = new ProcessBuilder("bq", "load", "public.accounts", "C:\\\\Users\\\\levav\\\\Documents\\\\data\\\\dataFile.csv", "account_id:integer,district_id:integer,frequency:string,account_date:date");
//        Process p = pb.start();
        Process process = Runtime.getRuntime().exec("cmd /c bq load public.accounts C:\\Users\\levav\\Documents\\data\\dataFile.csv account_id:integer,district_id:integer,frequency:string,account_date:date");
       // "bq load public.accounts C:\\Users\\levav\\Documents\\data\\dataFile.csv account_id:integer,district_id:integer,frequency:string,account_date:date"

//
// Records are passed as a map
//        Map<String, Object> recordsContent = new HashMap<>();
//        recordsContent.put("stringField", "Hello, World!");
//        rowContent.put("recordField", recordsContent);

//        Map<String, Object> rowContent = new HashMap<>();
//        String entry;
//        while ((entry = reader.readLine()) != null) {
//            String[] row = new String[4];
//            row = entry.split(",");
//            rowContent.put("account_id", row[0]);
//            rowContent.put("district_id", row[1]);
//            rowContent.put("frequency", row[2]);
//            rowContent.put("account_date", row[3]);
//
//            InsertAllResponse response =
//                    bigquery.insertAll(
//                            InsertAllRequest.newBuilder(tableId)
//                                    .addRow(rowContent)
//                                    // More rows can be added in the same RPC by invoking .addRow() on the builder.
//                                    // You can also supply optional unique row keys to support de-duplication scenarios.
//                                    .build());
//            rowContent.clear();
//            System.out.println("row = " + row[1] + " example = " + row[3]);
//        }
//        reader.close();
//        while (select.next()){
//            rowContent.put("account_id",select.getString(1));
//            rowContent.put("district_id",select.getString(2));
//            rowContent.put("frequency",select.getString(3));
//            rowContent.put("account_date",select.getString(4));
//
//            InsertAllResponse response =
//                    bigquery.insertAll(
//                            InsertAllRequest.newBuilder(tableId)
//                                    .addRow(rowContent)
//                                    // More rows can be added in the same RPC by invoking .addRow() on the builder.
//                                    // You can also supply optional unique row keys to support de-duplication scenarios.
//                                    .build());
//            rowContent.clear();
//            System.out.println("working");

//            if (response.hasErrors()) {
//                // If any of the insertions failed, this lets you inspect the errors
//                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
//                    // inspect row error
//                }
//            }

    }





    }







    //        String GetAllTables =
//"SELECT                                          \n" +
//        "  'CREATE TABLE ' || relname || E'\\n(\\n' ||\n" +
//        "  array_to_string(\n" +
//        "    array_agg(\n" +
//        "      '    ' || column_name || ' ' ||  type || ' '|| not_null\n" +
//        "    )\n" +
//        "    , E',\\n'\n" +
//        "  ) || E'\\n);\\n' as ddl_st\n" +
//        "from\n" +
//        "(\n" +
//        "  SELECT \n" +0
//        "    c.relname, a.attname AS column_name, pn.nspname as nspname ,\n" +
//        "    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,\n" +
//        "    case \n" +
//        "      when a.attnotnull\n" +
//        "    then 'NOT NULL' \n" +
//        "    else 'NULL' \n" +
//        "    END as not_null \n" +
//        "  FROM pg_class c,\n" +
//        "   pg_attribute a,\n" +
//        "   pg_type t, \n" +
//        "   pg_catalog.pg_namespace pn\n" +
//        "   \n" +
//        "   WHERE 1=1\n" +
//        "   and pn.oid=c.relnamespace \n" +
//        "   AND a.attnum > 0\n" +
//        "   AND a.attrelid = c.oid\n" +
//        "   AND a.atttypid = t.oid\n" +
//        "   and pn.nspname = ?\n" +
//        " ORDER BY a.attnum\n" +
//        ") as tabledefinition\n" +
//        "group by relname;";
//        GetAllTables = filter(GetAllTables);
//        System.out.println(GetAllTables);
//    }
//
//    public static String filter(String query){
//        String filtered = query.replaceAll("\\n","");
//        filtered = filtered.replaceAll("\\s\\s+","");
//        filtered = filtered.replaceAll(" NULL","");
//        return filtered;
//    }
//
//
//    Query query= new Query();
//    String host = "jdbc:postgresql://localhost:5432/";
//    // Move credentials to file
//
//    String userName = "postgres";
//    String password = "q1w2e3r4";
//    String projectId = "apeakdata";
//
//    List<Dataset> datasetList = new ArrayList<Dataset>();
//    File credentialsPath = new File("C:\\MyFolder\\ApeakData-4a7afc84ec8f.json");
//    DBConnect dbConnect = new DBConnect(host, userName, password);
//        dbConnect.Connect();
//    GoogleCredentials credentials = GoogleCredentials.newBuilder().build();
//    DataTypeConversion dataTypeConversion = new DataTypeConversion();
//    //connecting to BQ
//        try (
//    FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
//        credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
//    } catch (
//    IOException e) {
//        e.printStackTrace();
//    }
//    BigQuery bigquery =
//            BigQueryOptions.newBuilder()
//                    .setCredentials(credentials)
//                    .setProjectId(projectId)
//                    .build()
//                    .getService();
//
//    //End of connecting
//    ResultSet schemasResultset = dbConnect.executeQuery(query.getQueryByName("getSchemas"));
//
//        while (schemasResultset.next()){
//        String datasetName = schemasResultset.getString(1);
//        Dataset dataset = createDataset(bigquery, datasetName);
//        //creating tables
//        String getTablesQuery = query.getQueryByName("new");
//        String queryForTables = String.format(getTablesQuery,datasetName);
//        dbConnect.Reconnect();
//        ResultSet tablesResultset = dbConnect.executeQuery(queryForTables);
//        createTables(dataset,tablesResultset,dataTypeConversion);
//    }

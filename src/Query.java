import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Query {
    Map<String,String> queryMap;

    public Query(){
        this.queryMap = new HashMap<String, String>();
        this.init();
    }

    public Map<String, String> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(Map<String, String> queryMap) {
        this.queryMap = queryMap;
    }

    public String getQueryByName(String queryName){
        try{
            if(this.queryMap.containsKey(queryName)){
                return this.queryMap.get(queryName);
            }
        }catch (IllegalArgumentException e){
            throw new IllegalArgumentException("No such Query Exist");
        }
        return null;
    }

    public void put(String queryName, String query)  {
        try{
            if(!this.queryMap.containsKey(queryName)){
                this.queryMap.put(queryName,query);
            }
        }catch (IllegalArgumentException queryNameExists){
            throw  new IllegalArgumentException("Query Name Already exists");
        }
    }

    private void init(){
//        put("getTables",
//                "SELECT                                          \n" +
//                "  'CREATE TABLE ' || relname || E'\\n(\\n' ||\n" +
//                "  array_to_string(\n" +
//                "    array_agg(\n" +
//                "      '    ' || column_name || ' ' ||  type || ' '|| not_null\n" +
//                "    )\n" +
//                "    , E',\\n'\n" +
//                "  ) || E'\\n);\\n' as ddl_st\n" +
//                "from\n" +
//                "(\n" +
//                "  SELECT \n" +
//                "    c.relname, a.attname AS column_name, pn.nspname as nspname ,\n" +
//                "    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,\n" +
//                "    case \n" +
//                "      when a.attnotnull\n" +
//                "    then 'NOT NULL' \n" +
//                "    else 'NULL' \n" +
//                "    END as not_null \n" +
//                "  FROM pg_class c,\n" +
//                "   pg_attribute a,\n" +
//                "   pg_type t, \n" +
//                "   pg_catalog.pg_namespace pn\n" +
//                "   \n" +
//                "   WHERE 1=1\n" +
//                "   and pn.oid=c.relnamespace \n" +
//                "   AND a.attnum > 0\n" +
//                "   AND a.attrelid = c.oid\n" +
//                "   AND a.atttypid = t.oid\n" +
//                "   and pn.nspname = '%s' \n" +
//                " ORDER BY a.attnum\n" +
//                ") as tabledefinition\n" +
//                "group by relname;");

        put("getSchemas","select distinct nspname \n" +
                "from pg_catalog.pg_namespace pn\n" +
                "where pn.nspname not in ('information_schema') and pn.nspname not like 'pg%'");

        put("getTables","SELECT \n" +
                "    c.RELNAME as table_Name, a.attname AS field_Name,\n" +
                "    pg_catalog.format_type(a.atttypid, a.atttypmod) as field_type,\n" +
                "    row_number () OVER( partition by C.relname \n" +
                "    )as field_Number, COUNT(*) over (partition by  C.relname) as number_Of_Fields\n" +
                "\t  FROM pg_class c,\n" +
                "   pg_attribute a,\n" +
                "   pg_type t, \n" +
                "   pg_catalog.pg_namespace pn\n" +
                "  WHERE 1=1\n" +
                "   and pn.oid=c.relnamespace \n" +
                "   AND a.attnum > 0\n" +
                "   AND a.attrelid = c.oid\n" +
                "   AND a.atttypid = t.oid\n" +
                "   and pn.nspname = '%s'\n" +
                " ORDER by 1");
        put("select","select" +
                " * from %s;\n");
        put("number_of_columns","select \n" +
                "count(distinct a.attname) as num_of_columns\n" +
                "from pg_class c, pg_attribute as a\n" +
                "  WHERE 1=1\n" +
                "   AND a.attnum > 0\n" +
                "   AND a.attrelid = c.oid\n" +
                "   and c.RELNAME ='%s'");
    }

    public  void blabla(ResultSet resultSet) throws SQLException {
        while (resultSet.next()){
            System.out.println("Im a cool resultset");
        }
    }


    public   void again(ResultSet resultSet) throws SQLException {
        System.out.println(resultSet.next());
        while (resultSet.next()){
            System.out.println("again");
        }
    }
}

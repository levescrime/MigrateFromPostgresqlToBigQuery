import com.google.cloud.bigquery.*;

public class BigQueryConnect {
    BigQuery m_BigQuery ;
    DatasetId m_DatasetId;
    Dataset m_Dataset ;
    TableId m_TableId;
    TableDefinition m_TableDefinition;

    BigQueryConnect(String i_Project, String i_DataSet){
        this.m_BigQuery = BigQueryOptions.getDefaultInstance().getService();
        this.m_DatasetId = DatasetId.of(i_Project,i_DataSet);
    }

    Table CreateTable(String i_CreateString){
        return this.m_Dataset.create(i_CreateString,m_TableDefinition);
    }
}

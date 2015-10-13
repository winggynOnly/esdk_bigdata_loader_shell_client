package org.apache.loader.shell.client.option.submit;

public enum SubmitOptions
{
    // Commmon option
    JOB_NAME("n",""), UPD_JOB("u",""), JOB_TYPE("jobType",""), CONNECTOR_TYPE("connectorType",""), FRAMEWORK_TYPE("frameworkType",""),
    EXTRACTORS("extractors", "throttling.extractors"),
    
    // Generic jdbc connector option
    SCHEMA_NAME("schemaName", "table.schemaName"), TABLE_NAME("tableName", "table.tableName"), SQL("sql", "table.sql"), COLUMNS("columns", "table.columns"), PARTIION_COLUMN(
        "partitionColumn", "table.partitionColumn"), PARTIION_COLUMN_NULL("partitionColumnNull","table.partitionColumnNull"),STAGE_TABLE_NAME("stageTableName", "table.stageTableName"),
        
    // SFTP connector option
    INPUT_PATH("inputPath", "file.inputPath"), SUFFIX_NAME("suffixName", "file.suffixName"), ENCODE_TYPE("encodeType", "file.encodeType"), OUTPUT_PATH("outputPath", "file.outputPath"),
        
    // HDFS option
    OUTPUT_DIRECTORY("outputDirectory", "output.outputDirectory"), 
    // HBase optoin
    
    ;
    
    private String value;
    
    private String formKey;
    
    private SubmitOptions(String value, String formKey)
    {
        this.value = value;
        this.formKey = formKey;
    }
    
    public String getValue()
    {
        return value;
    }
    
    public String getFormKey()
    {
        return formKey;
    }
}

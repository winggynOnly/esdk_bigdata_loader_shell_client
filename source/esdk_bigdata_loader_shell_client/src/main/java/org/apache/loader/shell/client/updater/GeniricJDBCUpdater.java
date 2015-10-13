package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.loader.shell.client.option.submit.SubmitOptions;
import org.apache.sqoop.model.MJobForms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeniricJDBCUpdater extends Updater
{
    private static final Logger LOG = LoggerFactory.getLogger(GeniricJDBCUpdater.class);
    public GeniricJDBCUpdater(MJobForms jobForms, CommandLine commandLine)
    {
        super(jobForms, commandLine);
    }
    
    @Override
    public void update()
    {
        if (OptionValue.JobType.IMPORT.equals(jobType))
        {
            updateImportJob();
        }
        else
        {
            updateExportJob();
        }
    }
    
    private void updateImportJob()
    {
        String schemaNameOption= SubmitOptions.SCHEMA_NAME.getValue();
        if (hasOption(schemaNameOption))
        {
            updateValue(SubmitOptions.SCHEMA_NAME.getFormKey(), getOptionValue(schemaNameOption));
        }
        
        String tableNameOption= SubmitOptions.TABLE_NAME.getValue();
        if (hasOption(tableNameOption))
        {
            updateValue(SubmitOptions.TABLE_NAME.getFormKey(), getOptionValue(tableNameOption));
        }
        
        String sqlOption = SubmitOptions.SQL.getValue();
        if (hasOption(sqlOption))
        {
            updateValue(SubmitOptions.SQL.getFormKey(), getOptionValue(sqlOption));
            LOG.info("Update parameter. sql: {}", getOptionValue(sqlOption));
        }
        
        String columnsOption= SubmitOptions.COLUMNS.getValue();
        if (hasOption(columnsOption))
        {
            updateValue(SubmitOptions.COLUMNS.getFormKey(), getOptionValue(columnsOption));
        }
        
        String partitionColumnOption= SubmitOptions.PARTIION_COLUMN.getValue();
        if (hasOption(partitionColumnOption))
        {
            updateValue(SubmitOptions.PARTIION_COLUMN.getFormKey(), getOptionValue(partitionColumnOption));
        }
        
        String partitionColumnNullOption= SubmitOptions.PARTIION_COLUMN_NULL.getValue();
        if (hasOption(partitionColumnNullOption))
        {
            updateValue(SubmitOptions.PARTIION_COLUMN_NULL.getFormKey(), Boolean.parseBoolean(getOptionValue(partitionColumnNullOption)));
        }
    }
    
    private void updateExportJob()
    {
        String schemaNameOption= SubmitOptions.SCHEMA_NAME.getValue();
        if (hasOption(schemaNameOption))
        {
            updateValue(SubmitOptions.SCHEMA_NAME.getFormKey(), getOptionValue(schemaNameOption));
        }
        
        String tableNameOption= SubmitOptions.TABLE_NAME.getValue();
        if (hasOption(tableNameOption))
        {
            updateValue(SubmitOptions.TABLE_NAME.getFormKey(), getOptionValue(tableNameOption));
        }
        
        String stageTableOption= SubmitOptions.STAGE_TABLE_NAME.getValue();
        if (hasOption(stageTableOption))
        {
            updateValue(SubmitOptions.STAGE_TABLE_NAME.getFormKey(), getOptionValue(stageTableOption));
        }
    }
}

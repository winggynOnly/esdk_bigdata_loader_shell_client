package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.loader.shell.client.option.submit.SubmitOptions;
import org.apache.sqoop.model.MJobForms;

public class SftpUpdater extends Updater
{
    public SftpUpdater(MJobForms jobForms, CommandLine commandLine)
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
        String inputPathOption = SubmitOptions.INPUT_PATH.getValue();
        if (hasOption(inputPathOption))
        {
            updateValue(SubmitOptions.INPUT_PATH.getFormKey(), getOptionValue(inputPathOption));
        }
        
        String encodeTypeOption = SubmitOptions.ENCODE_TYPE.getValue();
        if (hasOption(encodeTypeOption))
        {
            updateValue(SubmitOptions.ENCODE_TYPE.getFormKey(), getOptionValue(encodeTypeOption));
        }
        
        String suffixNameOption = SubmitOptions.SUFFIX_NAME.getValue();
        if (hasOption(suffixNameOption))
        {
            updateValue(SubmitOptions.SUFFIX_NAME.getFormKey(), getOptionValue(suffixNameOption));
        }
    }
    
    private void updateExportJob()
    {
        String outputPath = SubmitOptions.OUTPUT_PATH.getValue();
        if (hasOption(outputPath))
        {
            updateValue(SubmitOptions.OUTPUT_PATH.getFormKey(), getOptionValue(outputPath));
        }
    }
}

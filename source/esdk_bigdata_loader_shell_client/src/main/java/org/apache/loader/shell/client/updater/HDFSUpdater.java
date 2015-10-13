package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.loader.shell.client.option.submit.SubmitOptions;
import org.apache.sqoop.model.MJobForms;

public class HDFSUpdater extends Updater
{
    public HDFSUpdater(MJobForms jobForms, CommandLine commandLine)
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
        String outputDirOption= SubmitOptions.OUTPUT_DIRECTORY.getValue();
        if (hasOption(outputDirOption))
        {
            updateValue(SubmitOptions.OUTPUT_DIRECTORY.getFormKey(), getOptionValue(outputDirOption));
        }
        
        String extractorsOption= SubmitOptions.EXTRACTORS.getValue();
        if (hasOption(extractorsOption))
        {
            int extractors = Integer.parseInt(getOptionValue(extractorsOption));
            updateValue(SubmitOptions.EXTRACTORS.getFormKey(), extractors);
        }
    }
    
    private void updateExportJob()
    {
        
    }
}

package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.loader.shell.client.ShellError;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.loader.shell.client.option.submit.SubmitOptions;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.utils.Preconditions;

public abstract class Updater
{
    protected MJobForms jobForms;
    
    protected CommandLine commandLine;
    
    protected OptionValue.JobType jobType;
    
    private OptionValue.JobType getJobType()
    {
        String option = SubmitOptions.JOB_TYPE.getValue();
        if (!commandLine.hasOption(option))
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "The job type is not configurated.");
        }
        
        String type = commandLine.getOptionValue(option);
        Preconditions.checkArgument(StringUtils.isNotBlank(type),
            ShellError.OPTIONS_INVALID,
            "The job type is not configurated.");
        
        if (type.equals(OptionValue.JobType.IMPORT.getType()))
        {
            return OptionValue.JobType.IMPORT;
        }
        else if (type.equals(OptionValue.JobType.EXPORT.getType()))
        {
            return OptionValue.JobType.EXPORT;
        }
        else
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID,
                String.format("The job type option is invalid. The valid values:%s,%s",
                    OptionValue.JobType.IMPORT.getType(),
                    OptionValue.JobType.EXPORT.getType()));
        }
    }
    
    protected boolean hasOption(String name)
    {
        if (commandLine.hasOption(name))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    protected String getOptionValue(String name)
    {
        String value = commandLine.getOptionValue(name);
        if (StringUtils.isBlank(value))
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, String.format("The option %s is not configurated.",
                name));
        }
        
        return value.trim();
    }
    
    protected void updateValue(String formKey, String value)
    {
        jobForms.getStringInput(formKey).setValue(value);
    }
    
    protected void updateValue(String formKey, boolean value)
    {
        jobForms.getBooleanInput(formKey).setValue(value);
    }
    
    protected void updateValue(String formKey, int value)
    {
        jobForms.getIntegerInput(formKey).setValue(value);
    }
    
    public Updater(MJobForms jobForms, CommandLine commandLine)
    {
        Preconditions.checkNotNull(commandLine, ShellError.PARAMETER_NULL, "commandLine");
        Preconditions.checkNotNull(jobForms, ShellError.PARAMETER_NULL, "commandLine");
        
        this.jobForms = jobForms;
        this.commandLine = commandLine;
        this.jobType = getJobType();
    }
    
    public abstract void update();
}

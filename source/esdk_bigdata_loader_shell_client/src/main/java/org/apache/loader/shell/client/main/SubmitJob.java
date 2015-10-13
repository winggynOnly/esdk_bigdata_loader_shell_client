package org.apache.loader.shell.client.main;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.loader.shell.client.Constants;
import org.apache.loader.shell.client.ShellClient;
import org.apache.loader.shell.client.ShellError;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.loader.shell.client.option.submit.SubmitOptions;
import org.apache.loader.shell.client.updater.Updater;
import org.apache.loader.shell.client.updater.UpdaterFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.validation.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubmitJob
{
    private static Logger LOG = LoggerFactory.getLogger(SubmitJob.class);
    
    /**
     * One second is equal 1000 milliseconds
     */
    private static final long ONE_SECOND = 1000;
    
    /**
     * One minute is equal 60 seconds
     */
    private static final long ONE_MINUT = 60;
    
    private SqoopClient sqoopClient = null;
    
    private CommandLine commandLine;
    
    private MJob mjob;
    
    private long jobId = -1;
    
    private String jobName;
    
    public SubmitJob(String[] args)
    {
        setLog4jConfig();
        
        ShellClient shellClient = new ShellClient();
        sqoopClient = shellClient.getClient();
        
        commandLine = getCommandLine(args);
        
        getJobInfo(commandLine);
    }
    
    private CommandLine getCommandLine(String[] args)
    {
        // Add options
        Options options = new Options();
        options.addOption(SubmitOptions.JOB_NAME.getValue(), null, true, "");
        options.addOption(SubmitOptions.UPD_JOB.getValue(), null, true, "");
        options.addOption(null, SubmitOptions.JOB_TYPE.getValue(), true, "");
        options.addOption(null, SubmitOptions.CONNECTOR_TYPE.getValue(), true, "");
        options.addOption(null, SubmitOptions.FRAMEWORK_TYPE.getValue(), true, "");
        options.addOption(null, SubmitOptions.SCHEMA_NAME.getValue(), true, "");
        options.addOption(null, SubmitOptions.TABLE_NAME.getValue(), true, "");
        options.addOption(null, SubmitOptions.SQL.getValue(), true, "");
        options.addOption(null, SubmitOptions.COLUMNS.getValue(), true, "");
        options.addOption(null, SubmitOptions.PARTIION_COLUMN.getValue(), true, "");
        options.addOption(null, SubmitOptions.PARTIION_COLUMN_NULL.getValue(), true, "");
        options.addOption(null, SubmitOptions.STAGE_TABLE_NAME.getValue(), true, "");
        options.addOption(null, SubmitOptions.INPUT_PATH.getValue(), true, "");
        options.addOption(null, SubmitOptions.ENCODE_TYPE.getValue(), true, "");
        options.addOption(null, SubmitOptions.SUFFIX_NAME.getValue(), true, "");
        options.addOption(null, SubmitOptions.OUTPUT_PATH.getValue(), true, "");
        options.addOption(null, SubmitOptions.OUTPUT_DIRECTORY.getValue(), true, "");
        options.addOption(null, SubmitOptions.EXTRACTORS.getValue(), true, "");
        
        CommandLineParser parser = new GnuParser();
        
        CommandLine commandLine = null;
        try
        {
            commandLine = parser.parse(options, args);
        }
        catch (ParseException e)
        {
            LOG.error("The options is invalid.", e);
            throw new SqoopException(ShellError.OPTIONS_INVALID);
        }
        
        return commandLine;
    }
    
    private void getJobInfo(CommandLine commmandLine)
    {
        if (!commmandLine.hasOption(SubmitOptions.JOB_NAME.getValue()))
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "The job name argument is not configurated.");
        }
        
        jobName = commmandLine.getOptionValue(SubmitOptions.JOB_NAME.getValue());
        if (StringUtils.isBlank(jobName))
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "The job name argument is not configurated.");
        }
        
        mjob = sqoopClient.getJob(jobName);
        if (mjob == null)
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, String.format("The job with name %s does not exist.",
                jobName));
        }
        
        jobId = mjob.getPersistenceId();
        if (jobId < 0)
        {
            throw new SqoopException(ShellError.SUBMIT_JOB_FAILED,
                String.format("The job id %s of job %s is incorrect.", jobId, jobName));
        }
        
        LOG.debug("The id of the job[{}] is {}", jobName, jobId);
    }
    
    private boolean updateJob()
    {
        OptionValue.ConnectorType connectorType = getConnectorType(commandLine);
        OptionValue.FrameworkType frameworkType = getFrameworkType(commandLine);
        
        if (connectorType != null)
        {
            MJobForms jobFormsConnectorPart = mjob.getConnectorPart();
            Updater connectorPart =
                UpdaterFactory.createConnectorPart(jobFormsConnectorPart, commandLine, connectorType);
            connectorPart.update();
        }
        
        if (frameworkType != null)
        {
            MJobForms jobFormsFrameworkPart = mjob.getFrameworkPart();
            Updater frameworkPart =
                UpdaterFactory.createFrameworkPart(jobFormsFrameworkPart, commandLine, frameworkType);
            frameworkPart.update();
        }
        
        Status status = sqoopClient.updateJob(mjob);
        if (status == null || !status.canProceed())
        {
            LOG.error("Failed to update the job {}", jobName);
            return false;
        }
        
        return true;
    }
    
    public boolean submit()
    {
        if (needUpdJob(commandLine) && (!updateJob()))
        {
            return false;
        }
        
        MSubmission submission = sqoopClient.startSubmission(jobId);
        SubmissionStatus submissionStatus = submission.getStatus();
        if (submissionStatus == null)
        {
            LOG.error("Failed to get the submission status.");
            return false;
        }
        
        if (submissionStatus.isFailure())
        {
            LOG.error("Failed to submit the job[{}]. Error: {}", jobName, submission.getExceptionInfo());
            return false;
        }
        
        LOG.info("Submit the job {} successfully.", jobName);
        
        do
        {
            long unknownTime = 0;
            do
            {
                submission = sqoopClient.getSubmissionStatus(jobId);
                if (submission != null)
                {
                    submissionStatus = submission.getStatus();
                }
                else
                {
                    LOG.warn("The submission is null.");
                    submissionStatus = SubmissionStatus.UNKNOWN;
                }
                
                sleepWhile(10 * ONE_SECOND);
                unknownTime += 10;
                if (unknownTime >= 30 * ONE_MINUT)
                {
                    break;
                }
                
                LOG.info(String.format("The submission status of job %s is %s. Progress:%.2f%%",
                    jobName,
                    submissionStatus,
                    (submission != null ? submission.getProgress()*100 : -1)));
            } while (submissionStatus == null || submissionStatus.isUnknown());
            
            sleepWhile(20 * ONE_SECOND);
            
        } while (SubmissionStatus.RUNNING.equals(submissionStatus) || SubmissionStatus.BOOTING.equals(submissionStatus));
        
        if (SubmissionStatus.SUCCEEDED.equals(submissionStatus))
        {
            return true;
        }
        else
        {
            if (submission == null)
            {
                LOG.error("Failed to submit job {}. Error: the submission is null", jobName);
                return false;
            }
            
            String error = submission.getExceptionInfo();
            if (error != null)
            {
                LOG.error("Failed to submit job {}. Error:{}", jobName, error);
            }
            
            return false;
        }
    }
    
    private OptionValue.ConnectorType getConnectorType(CommandLine commandLine)
    {
        if (!commandLine.hasOption(SubmitOptions.CONNECTOR_TYPE.getValue()))
        {
            return null;
        }
        
        String type = commandLine.getOptionValue(SubmitOptions.CONNECTOR_TYPE.getValue());
        if (OptionValue.ConnectorType.SFTP.getType().equals(type))
        {
            return OptionValue.ConnectorType.SFTP;
        }
        else if (OptionValue.ConnectorType.GENERIC_DB.getType().equals(type))
        {
            return OptionValue.ConnectorType.GENERIC_DB;
        }
        else
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "Unsupported connector type: " + type);
        }
    }
    
    private OptionValue.FrameworkType getFrameworkType(CommandLine commandLine)
    {
        if (!commandLine.hasOption(SubmitOptions.FRAMEWORK_TYPE.getValue()))
        {
            return null;
        }
        
        String type = commandLine.getOptionValue(SubmitOptions.FRAMEWORK_TYPE.getValue());
        if (OptionValue.FrameworkType.HDFS.getType().equals(type))
        {
            return OptionValue.FrameworkType.HDFS;
        }
        else if (OptionValue.FrameworkType.HBASE.getType().equals(type))
        {
            return OptionValue.FrameworkType.HBASE;
        }
        else
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "Unsupported framework type: " + type);
        }
    }
    
    private boolean needUpdJob(CommandLine commandLine)
    {
        if (!commandLine.hasOption(SubmitOptions.UPD_JOB.getValue()))
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID, "The update job argument is not configurated.");
        }
        
        String value = commandLine.getOptionValue(SubmitOptions.UPD_JOB.getValue());
        if (OptionValue.UpdateJob.YES.getValue().equals(value))
        {
            return true;
        }
        else if (OptionValue.UpdateJob.NO.getValue().equals(value))
        {
            return false;
        }
        else
        {
            throw new SqoopException(ShellError.OPTIONS_INVALID,
                String.format("The update job argument is invalid. The valid values:%s,%s",
                    OptionValue.UpdateJob.YES.getValue(),
                    OptionValue.UpdateJob.NO.getValue()));
        }
    }
    
    /**
     * 设置log4j的配置属性
     * 
     * @see 
     */
    private void setLog4jConfig()
    {
        //读取工具的安装路径
        String shellClientHome = System.getProperty(Constants.SHELL_CLIENT_HOME);
        if (StringUtils.isBlank(shellClientHome))
        {
            throw new SqoopException(ShellError.PARAMETER_EMPTY, "The shell client home is empty.");
        }
        
        String log4jFile = shellClientHome + Constants.LOG4J_FILE;
        if (!new File(log4jFile).exists())
        {
            throw new SqoopException(ShellError.PARAMETER_EMPTY, "The log4j file " + log4jFile + " does not exist.");
        }
        
        //设置log4j的配置文件
        PropertyConfigurator.configure(shellClientHome + Constants.LOG4J_FILE);
    }
    
    private static void sleepWhile(long time)
    {
        try
        {
            Thread.sleep(time);
        }
        catch (InterruptedException e)
        {
            LOG.error("", e);
        }
    }
    
    public static void main(String[] args)
    {
        SubmitJob submitJob = new SubmitJob(args);
        if (submitJob.submit())
        {
            System.exit(Constants.RESULT_SUCCESS);
        }
        else
        {
            System.exit(Constants.RESULT_FAILED);
        }
    }
}

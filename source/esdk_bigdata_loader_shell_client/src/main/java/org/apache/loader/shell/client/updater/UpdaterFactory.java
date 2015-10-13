package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.loader.shell.client.ShellError;
import org.apache.loader.shell.client.option.OptionValue;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.utils.Preconditions;

public class UpdaterFactory
{
    public static Updater createConnectorPart(MJobForms jobForms, CommandLine commandLine,
        OptionValue.ConnectorType connectorType)
    {
        Preconditions.checkNotNull(jobForms, ShellError.PARAMETER_NULL, "jobForms");
        Preconditions.checkNotNull(commandLine, ShellError.PARAMETER_NULL, "commandLine");
        Preconditions.checkNotNull(connectorType, ShellError.PARAMETER_NULL, "connectorType");
        
        switch (connectorType)
        {
            case SFTP:
                return new SftpUpdater(jobForms, commandLine);
                
            case GENERIC_DB:
                return new GeniricJDBCUpdater(jobForms, commandLine);
                
            default:
                throw new SqoopException(ShellError.OPTIONS_INVALID, "Unsupported connector type: "
                    + connectorType.getType());
        }
    }
    
    public static Updater createFrameworkPart(MJobForms jobForms, CommandLine commandLine,
        OptionValue.FrameworkType frameworkType)
    {
        Preconditions.checkNotNull(jobForms, ShellError.PARAMETER_NULL, "jobForms");
        Preconditions.checkNotNull(commandLine, ShellError.PARAMETER_NULL, "commandLine");
        Preconditions.checkNotNull(frameworkType, ShellError.PARAMETER_NULL, "frameworkType");
        
        switch (frameworkType)
        {
            case HDFS:
                return new HDFSUpdater(jobForms, commandLine);
                
            case HBASE:
                return new HBaseUpdater(jobForms, commandLine);
                
            default:
                throw new SqoopException(ShellError.OPTIONS_INVALID, "Unsupported framework type: "
                    + frameworkType.getType());
        }
    }
}

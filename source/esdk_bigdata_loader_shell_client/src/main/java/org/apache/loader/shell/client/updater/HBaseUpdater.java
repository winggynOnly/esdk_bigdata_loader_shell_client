package org.apache.loader.shell.client.updater;

import org.apache.commons.cli.CommandLine;
import org.apache.sqoop.model.MJobForms;

public class HBaseUpdater extends Updater
{
    public HBaseUpdater(MJobForms jobForms, CommandLine commandLine)
    {
        super(jobForms, commandLine);
    }

    @Override
    public void update()
    {
    }
}

package org.apache.loader.shell.client.option;

public class OptionValue
{
    public enum JobType
    {
        IMPORT("import"), EXPORT("export");
        
        private String type;
        
        private JobType(String type)
        {
            this.type = type;
        }
        
        public String getType()
        {
            return type;
        }
    }
    
    public enum ConnectorType
    {
        SFTP("sftp"), GENERIC_DB("rdb");
        
        private String type;
        
        private ConnectorType(String type)
        {
            this.type = type;
        }
        
        public String getType()
        {
            return type;
        }
    }
    
    public enum FrameworkType
    {
        HDFS("hdfs"), HBASE("hbase");
        
        private String type;
        
        private FrameworkType(String type)
        {
            this.type = type;
        }
        
        public String getType()
        {
            return type;
        }
    }
    
    public enum UpdateJob
    {
        YES("y"), NO("n");
        
        private String value;
        
        private UpdateJob(String type)
        {
            this.value = type;
        }
        
        public String getValue()
        {
            return value;
        }
    }
}

package org.apache.loader.shell.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellConfiguration
{
    private static final Logger LOG = LoggerFactory.getLogger(ShellConfiguration.class);
    
    private static final String SERVER_URL_KEY = "server.url";
    
    private static final String AUTH_TYPE_KEY = "authentication.type";
    
    private static final String USE_KEYTAB_KEY = "use.keytab";
    
    private static final String AUTH_USER_KEY = "authentication.user";
    
    private static final String AUTH_PWD_KEY = "authentication.password";
    
    private static final String PRINCIPAL_KEY = "client.principal";
    
    private static final String KEYTAB_FILE_KEY = "client.keytab.file";
    
    public static final String AUTH_TYPE_SIMPLE = "simple";
    
    private static final String AUTH_TYPE_KERBEROS = "kerberos";
    
    private String serverUrl;
    
    private String[] serverUrls = new String[2];
    
    private String authType;
    
    private String useKeytabStr;
    
    private String authUser;
    
    private String authPwd;
    
    private String principal;
    
    private String keytab;
    
    private boolean authByKerberos = true;
    
    private boolean useKeytab = true;
    
    private static final String CONFIG_FILE_NAME = "/conf/client.properties";
    
    public ShellConfiguration()
    {
        loadConf();
    }
    
    public void loadConf()
    {
        Properties prop = getProperties();
        
        serverUrl = prop.getProperty(SERVER_URL_KEY);
        authType = prop.getProperty(AUTH_TYPE_KEY);
        useKeytabStr = prop.getProperty(USE_KEYTAB_KEY);
        authUser = prop.getProperty(AUTH_USER_KEY);
        authPwd = prop.getProperty(AUTH_PWD_KEY);
        principal = prop.getProperty(PRINCIPAL_KEY);
        keytab = prop.getProperty(KEYTAB_FILE_KEY);
        
        String configInfo = new StringBuffer("Client properties.")
        .append("\nserver.url:").append(serverUrl)
        .append("\nauthentication.type:").append(authType)  
        .append("\nuse.keytab:").append(useKeytabStr)  
        .append("\nauthentication.user:").append(authUser)  
        .append("\nclient.principal:").append(principal)  
        .append("\nclient.keytab.file:").append(keytab)  
        .toString();
        LOG.debug(configInfo);
        
        validateInputParams();
    }
    
    private void validateInputParams()
    {
        // 校验必配项
        Preconditions.checkArgument(StringUtils.isNotBlank(serverUrl), ShellError.PARAMETER_EMPTY, SERVER_URL_KEY
            + " is not configurated.");
        serverUrl = serverUrl.trim();
        String[] servers = StringUtils.split(serverUrl, ",");
        Preconditions.checkArgument(servers.length == 2, ShellError.CONFIGURATION_INCORRECT, "The server.url maybe incorrect.");
        for (int i=0; i<2; i++)
        {
            serverUrls[i] = servers[i].trim();
        }
        
        Preconditions.checkArgument(StringUtils.isNotBlank(authType), ShellError.PARAMETER_EMPTY, AUTH_TYPE_KEY
            + " is not configurated.");
        authType = authType.trim();
        
        if (!AUTH_TYPE_SIMPLE.equals(authType) && !AUTH_TYPE_KERBEROS.equals(authType))
        {
            throw new SqoopException(ShellError.CONFIGURATION_INCORRECT,
                "Authentication type must be simple or kerberos. Type: " + authType);
        }
        
        if (!authType.equals(AUTH_TYPE_KERBEROS))
        {
            authByKerberos = false;
            return;
        }
        
        authByKerberos = true;
        
        Preconditions.checkArgument(StringUtils.isNotBlank(useKeytabStr), ShellError.PARAMETER_EMPTY, USE_KEYTAB_KEY
            + " is not configurated.");
        useKeytabStr = useKeytabStr.trim();
        
        Preconditions.checkArgument("true".equalsIgnoreCase(useKeytabStr) || "false".equalsIgnoreCase(useKeytabStr),
            ShellError.PARAMETER_EMPTY,
            USE_KEYTAB_KEY + "'s value must be ture or false.");
        useKeytab = new Boolean(useKeytabStr.toLowerCase()).booleanValue(); 
        
        if (useKeytab)
        {
            Preconditions.checkArgument(StringUtils.isNotBlank(principal), ShellError.PARAMETER_EMPTY, PRINCIPAL_KEY
                + " is not configurated.");
            principal = principal.trim();
            
            Preconditions.checkArgument(StringUtils.isNotBlank(keytab), ShellError.PARAMETER_EMPTY, KEYTAB_FILE_KEY
                + " is not configurated.");
            keytab = keytab.trim();
            
            Preconditions.checkArgument(new File(keytab).exists(),
                ShellError.LOAD_CONF_FILE_FAILURE,
                "The file " + keytab + " does not exist.");
        }
        else
        {
            Preconditions.checkArgument(StringUtils.isNotBlank(authUser), ShellError.PARAMETER_EMPTY, AUTH_USER_KEY
                + " is not configurated.");
            authUser = authUser.trim();
            
            Preconditions.checkArgument(StringUtils.isNotBlank(authPwd), ShellError.PARAMETER_EMPTY, AUTH_PWD_KEY
                + " is not configurated.");
            authPwd = authPwd.trim();
        }
    }
    
    private Properties getProperties()
    {
        // 读取工具安装路径
        String shellClientHome = System.getProperty(Constants.SHELL_CLIENT_HOME);
        if (StringUtils.isBlank(shellClientHome))
        {
            throw new SqoopException(ShellError.PARAMETER_EMPTY, "The shell client home is empty.");
        }
        
        File configFile = new File(shellClientHome + CONFIG_FILE_NAME);
        if (!configFile.exists())
        {
            throw new SqoopException(ShellError.PARAMETER_EMPTY, "The file " + configFile.getAbsolutePath() + " does not exist.");
        }
        
        Properties properties = new Properties();
        BufferedInputStream bufferedInputStream = null;
        try
        {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(configFile));
            properties.load(bufferedInputStream);
        }
        catch (IOException e)
        {
            throw new SqoopException(ShellError.LOAD_CONF_FILE_FAILURE, configFile.getAbsolutePath(), e);
        }
        finally
        {
            IOUtils.closeQuietly(bufferedInputStream);
        }
        
        return properties;
    }
    
    public boolean isAuthByKerberos()
    {
        return authByKerberos;
    }
    
    public void setAuthByKerberos(boolean authByKerberos)
    {
        this.authByKerberos = authByKerberos;
    }
    
    public String[] getServerUrls()
    {
        return serverUrls;
    }
    
    public String getAuthType()
    {
        return authType;
    }
    
    public String getAuthUser()
    {
        return authUser;
    }
    
    public String getAuthPwd()
    {
        return authPwd;
    }
    
    public String getPrincipal()
    {
        return principal;
    }
    
    public String getKeytab()
    {
        return keytab;
    }
    
    public boolean isUseKeytab()
    {
        return useKeytab;
    }
}

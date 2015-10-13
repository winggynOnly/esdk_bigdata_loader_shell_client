package org.apache.loader.shell.client;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.request.HealthRequest;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.transformation.HealthBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wcc.framework.util.encrypt.PasswordUtil;

public class ShellClient
{
    private static final Logger LOG = LoggerFactory.getLogger(ShellClient.class);
    
    private ShellConfiguration conf = new ShellConfiguration();
    
    private static String SQOOP_URL = "https://ip:port/loader/";
    
    private String IP_PORT = "ip:port";
    
    public SqoopClient getClient()
    {
        String[] servers = conf.getServerUrls();
        String activeUrl = "";
        
        for (String server : servers)
        {
            String url = SQOOP_URL.replace(IP_PORT, server);
            if (isActive(url))
            {
                activeUrl = url;
                break;
            }
        }
        
        if (StringUtils.isBlank(activeUrl))
        {
            throw new SqoopException(ShellError.SUBMIT_JOB_FAILED, "The loader service is unavailable.");
        }
        
        LOG.info("Current active loader server: {}", activeUrl);
        
        //实例化sqoop client对象
        SqoopClient client = null;
        
        if (ShellConfiguration.AUTH_TYPE_SIMPLE.equals(conf.getAuthType()))
        {
            return new SqoopClient(activeUrl);
        }
        
        if (conf.isUseKeytab())
        {
            client = new SqoopClient(activeUrl, conf.getPrincipal(), conf.getKeytab(), "");
        }
        else
        {
            String authPasswd = PasswordUtil.decryptByAes256(conf.getAuthPwd());
            client = new SqoopClient(activeUrl, conf.getAuthUser(), authPasswd);
        }
        
        return client;
    }
    
    private boolean isActive(String url)
    {
        HealthRequest healthRequest = new HealthRequest();
        HealthBean healthBean = null;
        try
        {
            healthBean = healthRequest.doGet(url);
        }
        catch (Exception e)
        {
            LOG.warn("Failed to get health status.", e);
        }
        
        if (null != healthBean)
        {
            if (null != healthBean.getHealth())
            {
                String state = healthBean.getHealth().getHaState();
                if ("active".equals(state))
                {
                    return true;
                }
            }
        }
        
        return false;
    }
}

package com.iflytek.oozie;


import java.util.Properties;

import org.apache.sqoop.client.SqoopClient;

import com.iflytek.oozie.sqoop2.action.Sqoop2Action;
import com.iflytek.oozie.sqoop2.utils.ResourceLoader;

/**
 * sqoop2迁移主类
 *
 */
public class Sqoop2Handler 
{
	// 初始化配置文件
	public static Properties properties = ResourceLoader
			.getResource("/sqoop2config.properties");

	 //初始化资源
    public static SqoopClient client = new SqoopClient(properties.getProperty("Sqoop2Url"));
  
    public static void main( String[] args )
    {
         new Sqoop2Action().sqoopTransfer();
    }
}

package com.iflytek.oozie;


import java.util.Properties;
import java.util.Set;

import org.apache.sqoop.client.SqoopClient;

import com.iflytek.oozie.sqoop2.action.Sqoop2Action;
import com.iflytek.oozie.sqoop2.utils.ResourceLoader;
import com.iflytek.oozie.sqoop2.utils.StringUtils;

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
    public static SqoopClient client ;
  
    public static void main( String[] args )
    {
    	
    	if (args.length>0) {
    		/*
        	 * 动态加载参数文件 如果参数文件中有参数 则直接用参数文件中的 
        	 * */
    		for (int i = 0; i < args.length; i++) {
    			if (!StringUtils.isNullOrEmpty(args[i])) {
         		//   Properties changedProp= ResourceLoader.getDirRessource(propertyUrl);
    				
    				String[] property=args[i].split("=");
    				if (property!=null &&property.length!=2) {
						continue;
					}
    				String key=args[i].split("=")[0];
    				String value=args[i].split("=")[1];
    				
    				if (properties.getProperty(key)!=null) {
    				     properties.setProperty(key, value);
					}
     		}
			}
    		
        	
		}
    
    	
      
    	 client=new SqoopClient(properties.getProperty("Sqoop2Url"));
         new Sqoop2Action().sqoopTransfer();
    }
}

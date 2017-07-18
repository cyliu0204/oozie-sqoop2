package com.iflytek.oozie.sqoop2.link;

import java.util.List;

import org.apache.sqoop.model.MLink;

import com.iflytek.oozie.Sqoop2Handler;
import com.iflytek.oozie.sqoop2.utils.StringUtils;



public abstract class LinkCreator {
	
	/**
	 * 配置字段
	 **/
	public static final String CONF_PREFIX = "oozie.sqoop2.link.LinkCreator.";

 	
    public static final String LINK_JDBC_URL = CONF_PREFIX + "JdbcUrl";

    public static final String LINK_JDBC_DRIVER = CONF_PREFIX + "JdbcDriver";

    public static final String LINK_JDBC_USER = CONF_PREFIX + "JdbcUser";

    public static final String LINK_JDBC_PASSWD = CONF_PREFIX + "JdbcPasswd";

    public static final String LINK_HDFS_URL = CONF_PREFIX + "HdfsUrl";
    
    public static final String LINK_KAKFA_BROKER=CONF_PREFIX+"BrokerList";
    
    public static final String LINK_KAFKA_ZOOKEEPER=CONF_PREFIX+ "ZookeeperConnect";
    
    public static final String LINK_HIVE_URL=CONF_PREFIX+"HiveUrl";
    
    public static final String LINK_HIVE_HDFSURI=CONF_PREFIX+"HdfsUri";
    
    public static final String LINK_HIVE_USER=CONF_PREFIX+"HiveUser";
    
    public static final String LINK_HIVE_PASSWD=CONF_PREFIX+"HivePasswd";
    
 
    
    
    public static final String NumExtractors = CONF_PREFIX + "NumExtractors";
	
	/**
	 *  查看是否已存在同名link
	 *  @param linkName
	 *  @return 返回同名mlink 如果没有返回空
	 **/
	public MLink checkIfLinkExists(String linkName) {
		List<MLink> mlinkList= Sqoop2Handler.client.getLinks();
		for (MLink mLink : mlinkList) {
			if (mLink.getName().equals(linkName)) {
				return mLink;
			}
		}
		return null;
	}

	public String getPropertyConfig(String key){
		return StringUtils.getNotNullString(Sqoop2Handler.properties.get(key));
	}
	
	
	public abstract MLink createLink();
	
}

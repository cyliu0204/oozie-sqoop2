package com.iflytek.oozie.sqoop2.link;

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;

public class JdbcLinkCreator extends LinkCreator{
	//是否创建link
    String isCreateLink,linkId,linkName,connectorName ;

	/**
	 * 创建link 初始化各个参数
	 * @param 是否创建 link
	 * @param 如果不创建 给出linkId
	 * @param 创建给出linkname和connnectorname 
	 **/
	public JdbcLinkCreator(String isCreateLink, String linkId, String linkName, String connectorName)
	{
		this.linkId=linkId;
		this.isCreateLink=isCreateLink;
		this.linkName=linkName;
		this.connectorName=connectorName;
		
	}
	@Override
	public MLink createLink( ) {
			//创建一个源链接 JDBC
			//检查是否已存在同名mlink
			MLink retLink=checkIfLinkExists(linkName);
			if (retLink!=null) {
				return retLink;
			}
	 		MLink Link = Sqoop2Handler.client.createLink(connectorName);
	 		Link.setName(linkName);
	 	    Link.setCreationUser("sqoop");
	 		MLinkConfig LinkConfig = Link.getConnectorLinkConfig();
	 		LinkConfig.getStringInput("linkConfig.connectionString").setValue(getPropertyConfig("JdbcUrl"));
	 		LinkConfig.getStringInput("linkConfig.jdbcDriver").setValue(getPropertyConfig("JdbcDriver"));
	 		LinkConfig.getStringInput("linkConfig.username").setValue(getPropertyConfig("JdbcUser"));
	 		LinkConfig.getStringInput("linkConfig.password").setValue(getPropertyConfig("JdbcPasswd"));
	 		Status Status = Sqoop2Handler. client.saveLink(Link);
	 		if(Status.canProceed()) {
	 		 System.out.println("创建JDBC Link成功，ID为: " + Link.getPersistenceId());
	 		} else {
	 		 System.out.println("创建JDBC Link失败");
	 		}
	 		return Link;
	}

}

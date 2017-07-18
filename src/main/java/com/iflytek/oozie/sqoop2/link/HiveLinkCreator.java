package com.iflytek.oozie.sqoop2.link;

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;

public class HiveLinkCreator extends LinkCreator{

	//是否创建link
    String isCreateLink,linkId,linkName,connectorName ;

	/**
	 * 创建link 初始化各个参数
	 * @param 是否创建 link
	 * @param 如果不创建 给出linkId
	 * @param 创建给出linkname和connnectorname 
	 **/
	public HiveLinkCreator(String isCreateLink, String linkId, String linkName, String connectorName)
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
	 		LinkConfig.getStringInput("linkConfig.hiveServerHost").setValue(getPropertyConfig(LINK_HIVE_URL));
	 		LinkConfig.getStringInput("linkConfig.hdfsURI").setValue(getPropertyConfig(LINK_HIVE_HDFSURI));
	 		LinkConfig.getStringInput("linkConfig.username").setValue(getPropertyConfig(LINK_HIVE_USER));
	 		LinkConfig.getStringInput("linkConfig.password").setValue(getPropertyConfig(LINK_HIVE_PASSWD));
	 		Status Status = Sqoop2Handler. client.saveLink(Link);
	 		if(Status.canProceed()) {
	 		 System.out.println("创建HIVE Link成功，Name为: " + Link.getName());
	 		} else {
	 		 System.out.println("创建HIVE Link失败");
	 		}
	 		return Link;
	}


}

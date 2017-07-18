package com.iflytek.oozie.sqoop2.link;

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;

public class HdfsLinkCreator extends LinkCreator{
	

	//是否创建link
    String isCreateLink,linkId,linkName,connectorName ;

	/**
	 * 创建link 初始化各个参数
	 * @param 是否创建 link
	 * @param 如果不创建 给出linkId
	 * @param 创建给出linkname和connnectorname 
	 **/
	public HdfsLinkCreator(String isCreateLink, String linkId, String linkName, String connectorName)
	{
		this.linkId=linkId;
		this.isCreateLink=isCreateLink;
		this.linkName=linkName;
		this.connectorName=connectorName;
		
	}
	 
	@Override
	public MLink createLink( ) {
		
		//检查是否已存在同名mlink
		MLink retLink=checkIfLinkExists(linkName);
		if (retLink!=null) {
			return retLink;
		}
     	//创建一个源链接 JDBC
    //	 long toConnectorId = 1;
          MLink hdfsLink = Sqoop2Handler.client.createLink(connectorName);
          hdfsLink.setName(linkName);
          hdfsLink.setCreationUser("sqoop");
          MLinkConfig toLinkConfig = hdfsLink.getConnectorLinkConfig();
          toLinkConfig.getStringInput("linkConfig.uri").setValue(getPropertyConfig(LINK_HDFS_URL));
          Status toStatus = Sqoop2Handler.client.saveLink(hdfsLink);
          if(toStatus.canProceed()) {
          System.out.println("创建HDFS Link成功，Name为: " + hdfsLink.getName());
           } else {
            System.out.println("创建HDFS Link失败");
          }
    	          
  		return hdfsLink;
      
	}
	 
}

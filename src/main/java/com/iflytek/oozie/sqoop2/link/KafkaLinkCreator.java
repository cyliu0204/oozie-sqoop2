package com.iflytek.oozie.sqoop2.link;

import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;

public class KafkaLinkCreator extends LinkCreator{

	

	//是否创建link
    String isCreateLink,linkId,linkName,connectorName ;

	/**
	 * 创建link 初始化各个参数
	 * @param 是否创建 link
	 * @param 如果不创建 给出linkId
	 * @param 创建给出linkname和connnectorname 
	 **/
	public KafkaLinkCreator(String isCreateLink, String linkId, String linkName, String connectorName)
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
     	//创建一个源链接 KafkaLink
    //	 long toConnectorId = 1;
          MLink kafkaLink = Sqoop2Handler.client.createLink(connectorName);
          kafkaLink.setName(linkName);
          kafkaLink.setCreationUser("sqoop");
          MLinkConfig toLinkConfig = kafkaLink.getConnectorLinkConfig();
          toLinkConfig.getStringInput("linkConfig.brokerList").setValue(getPropertyConfig(LINK_KAKFA_BROKER));
          toLinkConfig.getStringInput("linkConfig.zookeeperConnect").setValue(getPropertyConfig(LINK_KAKFA_BROKER));
          Status toStatus = Sqoop2Handler.client.saveLink(kafkaLink);
          if(toStatus.canProceed()) {
          System.out.println("创建Kafka Link成功，Name为: " + kafkaLink.getName());
           } else {
            System.out.println("创建Kafka Link失败");
          }
    	          
  		return kafkaLink;
      
	}
	 


}

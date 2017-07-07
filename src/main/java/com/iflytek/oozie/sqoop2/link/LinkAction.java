package com.iflytek.oozie.sqoop2.link;

import org.apache.sqoop.model.MLink;

import com.iflytek.oozie.Sqoop2Handler;

/**
 * 
 * @author lcy
 * @description link管理类
 *
 */
public class LinkAction  {
	
	//是否创建link
     String isCreateLink,linkId,linkName,connectorName ;
     
     LinkCreator linkCreator;
	/**
	 * 创建link 初始化各个参数
	 * @param 是否创建 link
	 * @param 如果不创建 给出linkId
	 * @param 创建给出linkname和connnectorname 
	 **/
	public LinkAction(String isCreateLink, String linkId, String linkName, String connectorName)
	{
		this.linkId=linkId;
		this.isCreateLink=isCreateLink;
		this.linkName=linkName;
		this.connectorName=connectorName;
		
	}
	
	
	//创建link
     public  MLink createLink(){
    	 //如果IsCreateLink 标志不为空 就默认需要创建link
    	 if (!isCreateLink.isEmpty()&&isCreateLink.equals("1")) {
    	 
    		if (connectorName.equals("generic-jdbc-connector")) {
    			linkCreator=new JdbcLinkCreator(isCreateLink, linkId, linkName, connectorName);
				return linkCreator.createLink();
			}else if(connectorName.equals("hdfs-connector")) {
				linkCreator=new HdfsLinkCreator(isCreateLink, linkId, linkName, connectorName);
				return linkCreator.createLink();
			
			}
		 }else {
			 if (!linkId.isEmpty()) {
				 return Sqoop2Handler.client.getLink(Long.valueOf(linkId));
			}else {
				throw new RuntimeException("LinkId 不能为空");
			}
		}
		return null;
     }
     
     
   
     
     
  
}

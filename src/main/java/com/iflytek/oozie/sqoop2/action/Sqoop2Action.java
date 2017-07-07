package com.iflytek.oozie.sqoop2.action;

import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

import com.iflytek.oozie.Sqoop2Handler;
import com.iflytek.oozie.sqoop2.job.JobAction;
import com.iflytek.oozie.sqoop2.link.LinkAction;
import com.iflytek.oozie.sqoop2.utils.StringUtils;

public class Sqoop2Action {
	
	
	/**
	 * 配置字段
	 */										  
	public static final String CONF_PREFIX = "oozie.sqoop2.action.Sqoop2Action.";

 	
    public static final String LINK_FROM_ISCREATE = CONF_PREFIX + "IsCreateFromLink";

    public static final String LINK_FROM_CONNECTORNAME = CONF_PREFIX + "FromConnectorName";

    public static final String LINK_FROM_LINKNAME = CONF_PREFIX + "FromLinkName";

    public static final String LINK_FROM_LINKID = CONF_PREFIX + "FromLinkId";

    /****to start****/

    public static final String LINK_TO_ISCREATE = CONF_PREFIX + "IsCreateToLink";

    public static final String LINK_TO_CONNECTORNAME = CONF_PREFIX + "ToConnectorName";

    public static final String LINK_TO_LINKNAME = CONF_PREFIX + "ToLinkName";

    public static final String LINK_TO_LINKID = CONF_PREFIX + "ToLinkId";
    /****to end****/

    
    public static final String NumExtractors = CONF_PREFIX + "NumExtractors";
	
	
	

	private String linkId = "", linkName = "", connectorName = "",
			isCreateLink = "";

	public void sqoopTransfer() {

		setFromPropertyConfig();
		MLink fromLink = new LinkAction(isCreateLink, linkId, linkName,
				connectorName).createLink();
		setToPropertyConfig();
		MLink toLink = new LinkAction(isCreateLink, linkId, linkName,
				connectorName).createLink();
		// 创建一个任务
		long fromLinkId = fromLink.getPersistenceId();
		long toLinkId = toLink.getPersistenceId();

		JobAction jobAction = new JobAction();

		MJob job = jobAction.createJob(fromLinkId, toLinkId);

		// 启动任务
		jobAction.startJob(job);

		System.out.println(" 通过sqoop传输数据 统计执行完毕");
	}

	public String getPropertyConfig(String key) {
		return StringUtils.getNotNullString(Sqoop2Handler.properties.get(key));
	}

	private void setFromPropertyConfig() {

		linkId = getPropertyConfig(LINK_FROM_LINKID);

		linkName = getPropertyConfig(LINK_FROM_LINKNAME);

		// connector 名字
		connectorName = getPropertyConfig(LINK_FROM_CONNECTORNAME);

		isCreateLink = getPropertyConfig(LINK_FROM_ISCREATE);
	}

	private void setToPropertyConfig() {

		linkId = getPropertyConfig(LINK_TO_LINKID);

		linkName = getPropertyConfig(LINK_TO_LINKNAME);

		// connector 名字
		connectorName = getPropertyConfig(LINK_TO_CONNECTORNAME);

		isCreateLink = getPropertyConfig(LINK_TO_ISCREATE);
	}
}

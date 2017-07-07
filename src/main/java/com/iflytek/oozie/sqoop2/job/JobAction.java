package com.iflytek.oozie.sqoop2.job;

import java.util.List;

import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;
import com.iflytek.oozie.sqoop2.action.Sqoop2Action;
import com.iflytek.oozie.sqoop2.utils.StringUtils;

public class JobAction {
	/**
	 * 配置字段
	 */
	public static final String CONF_PREFIX = "oozie.sqoop2.job.JobAction.";

	public static final String JOB_NAME = CONF_PREFIX+"JobName";

	
    public static final String JOB_JDBC_TO_TABLENAME = CONF_PREFIX + "ToTableName";

    public static final String JOB_JDBC_FROM_TABLENAME = CONF_PREFIX + "FromTableName";

    public static final String JOB_JDBC_TO_PARTITIONCOLUMN = CONF_PREFIX + "ToPartitionColumn";

    public static final String JOB_JDBC_FROM_PARTITIONCOLUMN = CONF_PREFIX + "FromPartitionColumn";
    
    public static final String JOB_JDBC_TO_SCHEMANAME = CONF_PREFIX + "ToSchemaName";

    public static final String JOB_JDBC_FROM_SCHEMANAME = CONF_PREFIX + "FromSchemaName";

    public static final String JOB_HDFS_FROM_HDFSURL = CONF_PREFIX + "FromHdfsUrl";

    public static final String JOB_HDFS_TO_HDFSURL = CONF_PREFIX + "ToHdfsUrl";

    
    public static final String NumExtractors = CONF_PREFIX + "NumExtractors";

	public String getPropertyConfig(String key) {
		return StringUtils.getNotNullString(Sqoop2Handler.properties.get(key));
	}
	/**
	 * @description：根据属性创建job
	 * 
	 */
	public MJob createJob(long fromLinkId, long toLinkId) {
		MJob job = Sqoop2Handler.client.createJob(fromLinkId, toLinkId);

		job.setName(getPropertyConfig(JOB_NAME));
	 	job.setCreationUser("sqoop");
		// 设置源链接任务配置信息
		setupMfromConfig(job);
		// 设置目的链接任务信息
		setupMToConfig(job);

		MDriverConfig driverConfig = job.getDriverConfig();
		driverConfig.getIntegerInput("throttlingConfig.numExtractors")
				.setValue(Integer.valueOf(getPropertyConfig(NumExtractors)));

		List<MJob> jobs = Sqoop2Handler.client.getJobs();
		boolean isExist = false;
		for (MJob mJob : jobs) {

			if (mJob.getName().equals(job.getName())) {
				isExist = true;
				job = mJob;
				break;
			}
		}
		if (!isExist) {
			Status status = Sqoop2Handler.client.saveJob(job);
			
			if (status.canProceed()) {
				System.out.println("JOB创建成功，ID为: " + job.getPersistenceId());
			} else {
				System.out.println("JOB创建失败。");
				return null;
			}
		}
		return job;
	}

     /**
      * 
      * @param job
      */
	public void startJob(MJob job) {
		long jobId = job.getPersistenceId();
		MSubmission submission = Sqoop2Handler.client.startJob(jobId);
		System.out.println("JOB提交状态为 : " + submission.getStatus());
		while (submission.getStatus().isRunning()
				&& submission.getProgress() != -1) {
			System.out.println("进度 : "
					+ String.format("%.2f %%", submission.getProgress() * 100));
			// 三秒报告一次进度
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("JOB执行结束... ...");
		System.out.println("Hadoop任务ID为 :" + submission.getExternalJobId());
		Counters counters = submission.getCounters();
		if (counters != null) {
			System.out.println("计数器:");
			for (CounterGroup group : counters) {
				System.out.print("\t");
				System.out.println(group.getName());
				for (Counter counter : group) {
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if (!submission.getStatus().isFailure()) {
			System.out.println("JOB执行成功");
		}
	}
	
	
	
	private void setupMToConfig(MJob job) {
		MToConfig toJobConfig = job.getToJobConfig();

		String connectorName = getPropertyConfig(Sqoop2Action.LINK_TO_CONNECTORNAME);
		// jdbc
		if (connectorName.equals("generic-jdbc-connector")) {

			toJobConfig.getStringInput("toJobConfig.schemaName").setValue(
					getPropertyConfig(JOB_JDBC_TO_SCHEMANAME));
			toJobConfig.getStringInput("toJobConfig.tableName").setValue(
					getPropertyConfig(JOB_JDBC_TO_TABLENAME));
			
			//可选项配置时候需要判断非空
			if (!getPropertyConfig(JOB_JDBC_TO_PARTITIONCOLUMN).isEmpty()) {
				toJobConfig.getStringInput("toJobConfig.partitionColumn")
				.setValue(getPropertyConfig(JOB_JDBC_TO_PARTITIONCOLUMN));
			}
			
			
		} else if (connectorName.equals("hdfs-connector")) {

			toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(
					getPropertyConfig(JOB_HDFS_TO_HDFSURL));

		}
	}

	// 根据选择源选择设置job属性
	private void setupMfromConfig(MJob job) {
		MFromConfig fromJobConfig = job.getFromJobConfig();
		String connectorName = getPropertyConfig(Sqoop2Action.LINK_FROM_CONNECTORNAME);
		// jdbc
		if (connectorName.equals("generic-jdbc-connector")) {

			fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue(
					getPropertyConfig(JOB_JDBC_FROM_SCHEMANAME));
			fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(
					getPropertyConfig(JOB_JDBC_FROM_TABLENAME));
			//可选项配置时候需要判断非空
			if (!getPropertyConfig(JOB_JDBC_FROM_PARTITIONCOLUMN).isEmpty()) {
				fromJobConfig.getStringInput("fromJobConfig.partitionColumn")
				.setValue(getPropertyConfig(JOB_JDBC_FROM_PARTITIONCOLUMN));
			}
			
		} else if (connectorName.equals("hdfs-connector")) {
			fromJobConfig.getStringInput("fromJobConfig.inputDirectory")
					.setValue(getPropertyConfig(JOB_HDFS_FROM_HDFSURL));
		}

	}
}

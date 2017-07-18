package com.iflytek.oozie.sqoop2.job;

import java.util.List;

import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import com.iflytek.oozie.Sqoop2Handler;
import com.iflytek.oozie.sqoop2.utils.StringUtils;

public class JobAction {
	/**
	 * 配置字段
	 */

	
	
	public static final String CONF_PREFIX = "oozie.sqoop2.job.JobAction.";

    
	public static final String JOB_NAME = CONF_PREFIX+"JobName";
	
	 public static final String NumExtractors = CONF_PREFIX + "NumExtractors";

	public String getPropertyConfig(String key) {
		return StringUtils.getNotNullString(Sqoop2Handler.properties.get(key));
	}
	/**
	 * @description：根据属性创建job
	 * 
	 */
	public MJob createJob(String fromLinkName, String toLinkName) {
		MJob job = Sqoop2Handler.client.createJob(fromLinkName, toLinkName);

		job.setName(getPropertyConfig(JOB_NAME));
	 	job.setCreationUser("sqoop");
		// 设置源链接任务配置信息
		JobConifg.setupMfromConfig(job);
		// 设置目的链接任务信息
		JobConifg.setupMToConfig(job);

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
				System.out.println("JOB创建成功，ID为: " + job.getName());
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
		String jobName = job.getName();
		MSubmission submission = Sqoop2Handler.client.startJob(jobName);
		System.out.println("JOB提交状态为 : " + submission.getStatus());
		submission=Sqoop2Handler.client.getJobStatus(jobName);
		while (submission.getStatus().isRunning()
				&& submission.getProgress() != -1) {
			submission=Sqoop2Handler.client.getJobStatus(jobName);
			System.out.println("进度 : "
					+ String.format("%.2f %%", Math.abs(submission.getProgress() * 100)));
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
			System.out.println("JOB 执行成功");
		}else {
			System.out.println("JOB 执行失败");
		}
	}
	
	
	
}

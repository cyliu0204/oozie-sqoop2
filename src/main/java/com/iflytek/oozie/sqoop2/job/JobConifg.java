package com.iflytek.oozie.sqoop2.job;

import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MToConfig;

import com.iflytek.oozie.Sqoop2Handler;
import com.iflytek.oozie.sqoop2.action.Sqoop2Action;
import com.iflytek.oozie.sqoop2.utils.StringUtils;

public class JobConifg {

	
    public static final String CONF_PREFIX_JDBC = "oozie.sqoop2.job.JobAction.jdbc.";

    public static final String CONF_PREFIX_HDFS = "oozie.sqoop2.job.JobAction.hdfs.";
    
    public static final String CONF_PREFIX_KAFKA = "oozie.sqoop2.job.JobAction.kafka.";
    
    public static final String CONF_PREFIX_HIVE = "oozie.sqoop2.job.JobAction.hive.";

	
    public static final String JOB_JDBC_TO_TABLENAME = CONF_PREFIX_JDBC + "hdfs.ToTableName";

    public static final String JOB_JDBC_FROM_TABLENAME = CONF_PREFIX_JDBC + "FromTableName";

    public static final String JOB_JDBC_TO_PARTITIONCOLUMN = CONF_PREFIX_JDBC + "ToPartitionColumn";

    public static final String JOB_JDBC_FROM_PARTITIONCOLUMN = CONF_PREFIX_JDBC + "FromPartitionColumn";
    
    public static final String JOB_JDBC_TO_SCHEMANAME = CONF_PREFIX_JDBC + "ToSchemaName";

    public static final String JOB_JDBC_FROM_SCHEMANAME = CONF_PREFIX_JDBC + "FromSchemaName";
    
    public static final String JOB_JDBC_FROM_CHECKCOLUMN = CONF_PREFIX_JDBC + "FromCheckColumn";
    
    public static final String JOB_JDBC_FROM_CHECKVALUE = CONF_PREFIX_JDBC + "FromCheckValue";
		    
		
	 

    public static final String JOB_HDFS_FROM_HDFSURL = CONF_PREFIX_HDFS + "FromHdfsUrl";

    public static final String JOB_HDFS_TO_HDFSURL = CONF_PREFIX_HDFS + "ToHdfsUrl";

    public static final String JOB_KAFKA_TO_TOPIC = CONF_PREFIX_KAFKA + "ToTopic";
 
    
    public static final String JOB_HIVE_TO_TABLENAME = CONF_PREFIX_HIVE + "ToTableName";

    public static final String JOB_HIVE_TO_OUTPUTDIR = CONF_PREFIX_HIVE + "OutputDir";

    public static final String JOB_HIVE_TO_NULLVALUE = CONF_PREFIX_HIVE + "ToNullValue";
    
    public static final String JOB_HIVE_TO_OVERRIDENULLVALUE = CONF_PREFIX_HIVE + "ToOverrideNullValue";
    
    
    public static final String JOB_HIVE_TO_APPENDMODE = CONF_PREFIX_HIVE + "ToAppendMode";

    public static final String JOB_HIVE_TO_COMPRESSION = CONF_PREFIX_HIVE + "TOCompressionCodec";

    public static final String JOB_HIVE_TO_HDFSFILEFORMAT = CONF_PREFIX_HIVE + "TOHdfsFileFormat";
    
    
   
    
    
    
    public static final String JOB_HIVE_FROM_TABLENAME = CONF_PREFIX_HIVE + "FromTableName";
    
    public static final String JOB_HIVE_FROM_INPUTDIR = CONF_PREFIX_HIVE + "InputDir";

    public static final String JOB_HIVE_FROM_NULLVALUE = CONF_PREFIX_HIVE + "FromNullValue";
    
    public static final String JOB_HIVE_FROM_OVERRIDENULLVALUE = CONF_PREFIX_HIVE + "FromOverrideNullValue"; 
    
    /**
	 * 设置目标job的的配置
	 * @param job
	 */
	public static void setupMToConfig(MJob job) {
		MToConfig toJobConfig = job.getToJobConfig();

		String connectorName = getPropertyConfig(Sqoop2Action.LINK_TO_CONNECTORNAME);
		// jdbc
		if (connectorName.equals("generic-jdbc-connector")) {
			
			setUpToJdbcJobConfig(toJobConfig);
			
		} else if (connectorName.equals("hdfs-connector")) {
			
			setUpToHdfsJobConfig(toJobConfig);

		} else if (connectorName.equals("kafka-connector")) {
			
			setUpToKafkaJobConfig(toJobConfig);
	
		}else if(connectorName.equals("hive-connector")) {
			
			setUpToHiveJobConfig(toJobConfig);
		
		}
	}


	/**
	 * 设置源job的的配置
	 * @param job
	 */
	public static void setupMfromConfig(MJob job) {
		MFromConfig fromJobConfig = job.getFromJobConfig();
		String connectorName = getPropertyConfig(Sqoop2Action.LINK_FROM_CONNECTORNAME);
		// jdbc
		if (connectorName.equals("generic-jdbc-connector")) {

			setUpFromJdbcJobConfig(fromJobConfig);
			
		} else if (connectorName.equals("hdfs-connector")) {
			
			setUpFromHdfsConfig(fromJobConfig);
		
		}else if (connectorName.equals("hive-connector")) {
			
			setUpFromHiveJobConfig(fromJobConfig);
			
		}

	}


	private static void setUpFromHdfsConfig(MFromConfig fromJobConfig) {
		fromJobConfig.getStringInput("fromJobConfig.inputDirectory")
				.setValue(getPropertyConfig(JOB_HDFS_FROM_HDFSURL));
	}


	private static void setUpFromJdbcJobConfig(MFromConfig fromJobConfig) {
		fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue(
				getPropertyConfig(JOB_JDBC_FROM_SCHEMANAME));
		fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(
				getPropertyConfig(JOB_JDBC_FROM_TABLENAME));
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_JDBC_FROM_PARTITIONCOLUMN).isEmpty()) {
			fromJobConfig.getStringInput("fromJobConfig.partitionColumn")
			.setValue(getPropertyConfig(JOB_JDBC_FROM_PARTITIONCOLUMN));
		}
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_JDBC_FROM_CHECKCOLUMN).isEmpty()) {
			fromJobConfig.getStringInput("incrementalRead.checkColumn")
			.setValue(getPropertyConfig(JOB_JDBC_FROM_CHECKCOLUMN));
		}
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_JDBC_FROM_CHECKVALUE).isEmpty()) {
			fromJobConfig.getStringInput("incrementalRead.lastValue")
			.setValue(getPropertyConfig(JOB_JDBC_FROM_CHECKVALUE));
		}
		
		
	}
	

	private static void setUpFromHiveJobConfig(MFromConfig fromJobConfig) {
		
		if (!getPropertyConfig(JOB_HIVE_TO_OVERRIDENULLVALUE).isEmpty()) {
			fromJobConfig.getBooleanInput("fromJobConfig.overrideNullValue").setValue(Boolean.valueOf(
					getPropertyConfig(JOB_HIVE_TO_OVERRIDENULLVALUE)));
		}
		
		if (!getPropertyConfig(JOB_HIVE_TO_NULLVALUE).isEmpty()) {
			fromJobConfig.getStringInput("fromJobConfig.nullValue").setValue(
					getPropertyConfig(JOB_HIVE_TO_NULLVALUE));
		}
		
		
		fromJobConfig.getStringInput("fromJobConfig.inputDirectory").setValue(
				getPropertyConfig(JOB_HIVE_TO_OUTPUTDIR));
		fromJobConfig.getStringInput("fromJobConfig.querySql").setValue(
				getPropertyConfig(JOB_HIVE_TO_TABLENAME));
	}
	
	
	private static void setUpToJdbcJobConfig(MToConfig toJobConfig) {
		toJobConfig.getStringInput("toJobConfig.schemaName").setValue(
				getPropertyConfig(JOB_JDBC_TO_SCHEMANAME));
		toJobConfig.getStringInput("toJobConfig.tableName").setValue(
				getPropertyConfig(JOB_JDBC_TO_TABLENAME));
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_JDBC_TO_PARTITIONCOLUMN).isEmpty()) {
			toJobConfig.getStringInput("toJobConfig.partitionColumn")
			.setValue(getPropertyConfig(JOB_JDBC_TO_PARTITIONCOLUMN));
		}
	}
	private static void setUpToHiveJobConfig(MToConfig toJobConfig) {
		
		if (!getPropertyConfig(JOB_HIVE_TO_OVERRIDENULLVALUE).isEmpty()) {

			toJobConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(
				Boolean.valueOf(getPropertyConfig(JOB_HIVE_TO_OVERRIDENULLVALUE))	);
		}
		
		if (!getPropertyConfig(JOB_HIVE_TO_NULLVALUE).isEmpty()) {
			
			toJobConfig.getStringInput("toJobConfig.nullValue").setValue(
					getPropertyConfig(JOB_HIVE_TO_NULLVALUE));
		}
		
		
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(
				getPropertyConfig(JOB_HIVE_TO_OUTPUTDIR));
		
		
		toJobConfig.getStringInput("toJobConfig.tableName").setValue(
				getPropertyConfig(JOB_HIVE_TO_TABLENAME));
		
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_HIVE_TO_APPENDMODE).isEmpty()) {
			toJobConfig.getBooleanInput("toJobConfig.appendMode").setValue(Boolean.valueOf(getPropertyConfig(JOB_HIVE_TO_APPENDMODE)));
		}
				
		
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_HIVE_TO_HDFSFILEFORMAT).isEmpty()) {
			toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue(getPropertyConfig(JOB_HIVE_TO_HDFSFILEFORMAT));
		}
		
		//可选项配置时候需要判断非空
		if (!getPropertyConfig(JOB_HIVE_TO_COMPRESSION).isEmpty()) {
			toJobConfig.getEnumInput("toJobConfig.compression").setValue(getPropertyConfig(JOB_HIVE_TO_COMPRESSION));
		}
		
	}
	
	
	
	private static void setUpToKafkaJobConfig(MToConfig toJobConfig) {
		toJobConfig.getStringInput("toJobConfig.topic").setValue(
				getPropertyConfig(JOB_KAFKA_TO_TOPIC));
	}
	private static void setUpToHdfsJobConfig(MToConfig toJobConfig) {
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(
				getPropertyConfig(JOB_HDFS_TO_HDFSURL));
	}


	public static String getPropertyConfig(String key) {
		return StringUtils.getNotNullString(Sqoop2Handler.properties.get(key));
	}
}

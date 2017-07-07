package com.iflytek.oozie.sqoop2.utils;

public class StringUtils {
	
	//判断object是否为空
	public static boolean isNullOrEmpty(String str){
	
		return str==null?true:str.toString().isEmpty();
	
	}
	
	
	//将object转换为字符串 为空则返回空
	
	public static String getNotNullString(Object str) {
		
		return str==null?"":str.toString();
	
	}
}

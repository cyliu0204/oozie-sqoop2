package com.iflytek.oozie.sqoop2.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/*
 * 资源加载类
 * @author:lcy
 * @date:2017/6.16
 */
public class ResourceLoader {
	public static Properties getResource(String url){
		 Properties prop = new Properties();   
         InputStream in = Object.class.getResourceAsStream(url);   
         try {   
             prop.load(in);   
             System.out.println("开始加载文件");
             return prop;
            
         } catch (IOException e) {   
             e.printStackTrace();   
             try {
                 in.close();
             } catch (IOException e1) {
                 // TODO Auto-generated catch block
                 e1.printStackTrace();
             }
         } 
         return null;
	}
	
	
	
	//获取绝对路径resouce
	public static Properties getDirRessource(String url) {
		 Properties prop = new Properties();   
         InputStream in = null;
		try {
			in = new FileInputStream(url);
             prop.load(in);   
             System.out.println("开始加载文件");
             return prop;
            
         } catch (IOException e) {   
             e.printStackTrace();   
             try {
                 in.close();
             } catch (IOException e1) {
                 // TODO Auto-generated catch block
                 e1.printStackTrace();
             }
         }
         return null;
	}
}

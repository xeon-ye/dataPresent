package com.tiger.utils;

import org.jasypt.encryption.pbe.PooledPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.SimpleStringPBEConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.alibaba.druid.util.StringUtils;

@Service
public class JasyptUtils {
	@Value("${jasypt.encryptor.password}")
	private String password;
	@Value("${jasypt.encryptor.algorithm}")
    private String algorithm;
    
    public String encypt(String value,String newPswd){
        PooledPBEStringEncryptor encryptor = new PooledPBEStringEncryptor();
        encryptor.setConfig(cryptor(newPswd));
        String result = encryptor.encrypt(value);
        return result;
    }
    
    public String decypt(String value,String newPswd){
        PooledPBEStringEncryptor encryptor = new PooledPBEStringEncryptor();
        encryptor.setConfig(cryptor(newPswd));
        String result = encryptor.decrypt(value);
        return result;
    }

    public SimpleStringPBEConfig cryptor(String newPswd){
        SimpleStringPBEConfig config = new SimpleStringPBEConfig();
        if(StringUtils.isEmpty(newPswd)){
        	config.setPassword(password);
        }else{
        	config.setPassword(newPswd);
        }
        config.setAlgorithm(algorithm);
        config.setKeyObtentionIterations("1000");
        config.setPoolSize("1");
        config.setProviderName("SunJCE");
        config.setSaltGeneratorClassName("org.jasypt.salt.RandomSaltGenerator");
        config.setIvGeneratorClassName("org.jasypt.iv.RandomIvGenerator");
        config.setStringOutputType("base64");
        return config;
    }
    
    public String[] parseDataPros(String wholeProName){
    	String[] dtPros = new String[]{"",""};
    	if(StringUtils.isEmpty(wholeProName)){
    		return dtPros;
    	}
    	int spIndex = wholeProName.indexOf("@");
    	if(spIndex<0){
    		dtPros[0]="";
    		dtPros[1]=wholeProName;
    	}else if(spIndex==0){
    		dtPros[0]="";
    		dtPros[1]=wholeProName.substring(spIndex+1,wholeProName.length());
    	}else{
    		dtPros[0]=wholeProName.substring(0,spIndex);
    		dtPros[1]=wholeProName.substring(spIndex+1,wholeProName.length());
    	}
    	return dtPros;
    }
}

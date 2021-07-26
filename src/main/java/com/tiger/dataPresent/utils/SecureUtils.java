package com.tiger.dataPresent.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.tiger.utils.BeanConfigUtils;

@Service("secureUtils")  
public class SecureUtils {
	private static Logger log = LoggerFactory.getLogger(SecureUtils.class);
	//初始化向量(IV)，aes 16位
    private static final String IV = "0123456789abcdef";
    //此处两个密钥是解密数据库的加密字段用，受数据库加密密钥约束
	@Value("${KEY_DES3:37C50DD06617463FAF1FBF8E3B34DC23D46297E05E23D36C}")
	private String keyDES3;
	
	@Value("${KEY_AES:8F93E35572BA03E234FC74C9F75A074A}")
	private String keyAES;
	@Autowired
	private ZipService zipService;
	
    public String encrypt_DES3(String input) throws GeneralSecurityException, UnsupportedEncodingException {
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(keyDES3.toCharArray());
        } catch (Exception e) {
          e.printStackTrace();
        }
        Cipher cipher = Cipher.getInstance("DESede/CBC/NoPadding");
        byte[] bytes = input.getBytes("UTF-8");
    	SecretKey key = new SecretKeySpec(encryptionKey, "DESede");
    	IvParameterSpec iv = null;
    	try{
        	iv = new IvParameterSpec(Hex.decodeHex(IV.toCharArray()));
        }catch(Exception e){
        	log.error("初始化解密算法时发生错误："+e.toString());
        }
    	bytes = Arrays.copyOf(bytes, ((bytes.length+7)/8)*8);
    	cipher.init(Cipher.ENCRYPT_MODE, (Key)key, iv);
    	byte[] ebs = cipher.doFinal(bytes);
        return new String(Hex.encodeHex(ebs));
    }

    public String decrypt_DES3(String input) throws GeneralSecurityException, DecoderException, UnsupportedEncodingException {
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(keyDES3.toCharArray());
        }catch (Exception e) {
        	e.printStackTrace();
        }
        Cipher cipher =Cipher.getInstance("DESede/CBC/NoPadding");
    	byte[] bytes = Hex.decodeHex(input.toCharArray());
    	SecretKey key = new SecretKeySpec(encryptionKey, "DESede");
    	IvParameterSpec iv = null;
    	try{
        	iv = new IvParameterSpec(Hex.decodeHex(IV.toCharArray()));
        }catch(Exception e){
        	log.error("初始化解密算法时发生错误："+e.toString());
        }
    	cipher.init(Cipher.DECRYPT_MODE, (Key)key, iv);
        byte[] dbs = cipher.doFinal(bytes);
        String decrypted = new String(dbs, "UTF-8");
        if (decrypted.indexOf((char)0) > 0) {
            decrypted = decrypted.substring(0, decrypted.indexOf((char)0));
        }
        return decrypted;
    }
    
    public String encrypt_AES(String content) {
    	byte[] result = null;
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(keyAES.toCharArray());
        }catch (Exception e) {
        	e.printStackTrace();
        }
        try {
        	Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(encryptionKey,"AES");
            //向量iv
            byte[] ivs = IV.getBytes("UTF-8");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(ivs);
            cipher.init(Cipher.ENCRYPT_MODE,keySpec,ivParameterSpec);
            byte[] byteContent = content.getBytes("utf-8");
            result = cipher.doFinal(byteContent);       
        }catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }catch (InvalidKeyException e) {
            e.printStackTrace();
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        }catch (BadPaddingException e) {
            e.printStackTrace();
        }catch(Exception e){
        	e.printStackTrace();
        }
        return new String(Hex.encodeHex(result));
    }
    
    public String decrypt_AES(String input) {
    	byte[] result = null;
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(keyAES.toCharArray());
        }catch (Exception e) {
        	e.printStackTrace();
        }
        try {
        	Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(encryptionKey,"AES");
            //向量iv
            byte[] ivs = IV.getBytes("UTF-8");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(ivs);
            cipher.init(Cipher.DECRYPT_MODE,keySpec,ivParameterSpec);
            byte[] content = Hex.decodeHex(input.toCharArray());
            result = cipher.doFinal(content);  
            return new String(result,"utf-8");
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }catch (InvalidKeyException e) {
            e.printStackTrace();
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        }catch (BadPaddingException e) {
            e.printStackTrace();
        }catch(Exception e){
        	e.printStackTrace();
        }
        return null;
    }
    
    public String encrypt_AES(String key,String content) {
    	byte[] result = null;
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(key.toCharArray());
        }catch (Exception e) {
        	e.printStackTrace();
        }
        try {
        	Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(encryptionKey,"AES");
            //向量iv
            byte[] ivs = IV.getBytes("UTF-8");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(ivs);
            cipher.init(Cipher.ENCRYPT_MODE,keySpec,ivParameterSpec);
            byte[] byteContent = content.getBytes("utf-8");
            result = cipher.doFinal(byteContent);       
        }catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }catch (InvalidKeyException e) {
            e.printStackTrace();
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        }catch (BadPaddingException e) {
            e.printStackTrace();
        }catch(Exception e){
        	e.printStackTrace();
        }
        return new String(Hex.encodeHex(result));
    }
    public String decrypt_AES(String key,String input) {
    	byte[] result = null;
    	byte[] encryptionKey = null;
        try {
        	encryptionKey = Hex.decodeHex(key.toCharArray());
        }catch (Exception e) {
        	e.printStackTrace();
        }
        try {
        	Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec keySpec = new SecretKeySpec(encryptionKey,"AES");
            //向量iv
            byte[] ivs = IV.getBytes("UTF-8");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(ivs);
            cipher.init(Cipher.DECRYPT_MODE,keySpec,ivParameterSpec);
            byte[] content = Hex.decodeHex(input.toCharArray());
            result = cipher.doFinal(content);  
            return new String(result,"utf-8");
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }catch (NoSuchPaddingException e) {
            e.printStackTrace();
        }catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }catch (InvalidKeyException e) {
            e.printStackTrace();
        }catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        }catch (BadPaddingException e) {
            e.printStackTrace();
        }catch(Exception e){
        	e.printStackTrace();
        }
        return null;
    }
    
    
    public String decipher(String input,String algorithm){
    	String decrypted = input;
    	try{
	    	if("DES3".equalsIgnoreCase(algorithm)){
	    		decrypted = decrypt_DES3(input);
	    	}else{
	    		decrypted = decrypt_AES(input);
	    	}
    	}catch(Exception e){
        	log.error("解密时发生错误："+e.toString());
        }
    	return decrypted;
    }
    
    /** 
     * 生成密钥 
     * 自动生成AES128位密钥 
     * filePath 表示存储密钥的文件路径
     */  
    public String createAESKey(int bit){
    	String key ="";
    	try{
    		//实例化一个用AES加密算法的密钥生成器
    		KeyGenerator kg = KeyGenerator.getInstance("AES"); 
    		//要生成多少位，只需要修改这里即可128, 192或256。生成128位随机密钥(如要生成256位需要替换jar包(jdk默认支持128位)) 
            kg.init(bit, new SecureRandom());
            //生成一个密钥。
            SecretKey sk = kg.generateKey();
            //返回基本编码格式的密钥，如果此密钥不支持编码，则返回 null。
            byte[] b = sk.getEncoded();
            key = new String(Hex.encodeHex(b));
    	}catch(Exception e){
    	}
        return key;
    }  
      
    /** 
     * 加密 
     * 使用对称密钥进行加密 
     * keyFilePath 密钥存放路径 
     * sourseFile 要加密的文件
     * encodeFile 加密后的文件
     */  
    public void fileEncodeAES(String keyStr,String filepath) throws Exception{
        byte[] key = Hex.decodeHex(keyStr.toCharArray());
        //根据给定的字节数组(密钥数组)构造一个AES密钥。
        SecretKeySpec sKeySpec = new SecretKeySpec(key, "AES");
        //实例化一个密码器（CBC模式）
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        //初始化密码器
        cipher.init(Cipher.ENCRYPT_MODE, sKeySpec, new IvParameterSpec(IV.getBytes("UTF-8")));
        List files = getFiles( filepath);
        if(files==null||files.size()==0){
        	return ;
        }
        for(int i=0;i<files.size();i++){
        	String filename = (String)files.get(i);
        	//读取要加密的文件流
        	File file = new File(filename);
            FileInputStream inputStream = new FileInputStream(file);
            //输出加密后的文件流
            String fn = filename.substring(0,filename.indexOf("."));
            String suffix =filename.substring(filename.indexOf(".")+1,filename.length());
            FileOutputStream outputStream = new FileOutputStream(fn+"_e"+"."+suffix);
            //以加密流写入文件  
            CipherInputStream cipherInputStream = new CipherInputStream(inputStream, cipher);
            byte[] b = new byte[1024];
            int len = 0;
            //没有读到文件末尾一直读
            while((len = cipherInputStream.read(b)) != -1) {
                outputStream.write(b, 0, len);
                outputStream.flush();
            }
            cipherInputStream.close();
            inputStream.close();
            file.delete();
            outputStream.close();
        }
    }  
    /** 
     * 解密 
     * 使用对称密钥进行解密 
     * keyStr 密钥 
     * encodeFile 要解密的文件
     * decodeFile 解密后的文件
     */  
    public void fileDecodeAES(String keyStr,String filepath) throws Exception{
        //读取保存密钥的文件
    	byte[] key = Hex.decodeHex(keyStr.toCharArray());
        //根据密钥文件构建一个AES密钥
        SecretKeySpec sKeySpec = new SecretKeySpec(key, "AES");
        //实例化一个密码器CBC模式的
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        //初始化密码器
        cipher.init(Cipher.DECRYPT_MODE, sKeySpec, new IvParameterSpec(IV.getBytes("UTF-8")));
        List files = getFiles( filepath);
        if(files==null||files.size()==0){
        	return ;
        }
        for(int i=0;i<files.size();i++){
        	String filename = (String)files.get(i);
            //解密文件流
            String fn = filename.substring(0,filename.indexOf("."));
            String suffix =filename.substring(filename.indexOf(".")+1,filename.length());
            if("zip".equalsIgnoreCase(suffix)){
            	String decodePath = fn+File.separator;
            	zipService.decompressZip(filename, fn);
            	File dcRoot = new File(decodePath);
        	    File [] dfiles = dcRoot.listFiles();  
        	    for(File f : dfiles){ 
        	    	decodeFile(f.getAbsolutePath(),cipher);
        	    }
            	
            }else{
            	decodeFile(filename,cipher);
            }
        }
    }
    private void decodeFile(String filename,Cipher cipher){
    	//解密文件流
        String dir = filename.substring(0,filename.lastIndexOf(File.separator));
        String realName = filename.substring(filename.lastIndexOf(File.separator)+1,filename.length());
        String decodePath=dir+File.separator+"decode"+File.separator;
    	//加密文件流  
        try{
	        FileInputStream inputStream = new FileInputStream(filename);
	        File decodeRoot = new File(decodePath);
	        if(!decodeRoot.exists()) {
	        	decodeRoot.mkdirs();
            }
	        File decodeFile = new File(decodePath + realName);
	        decodeFile.createNewFile();
	        FileOutputStream outputStream = new FileOutputStream(decodeFile);
	        //以解密流写出文件  
	        CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, cipher);  
	        byte[] buffer = new byte[1024];  
	        int r;  
	        while ((r = inputStream.read(buffer)) >= 0) {  
	            cipherOutputStream.write(buffer, 0, r);  
	        }  
	        cipherOutputStream.close();
	        inputStream.close();
	        outputStream.close();
        }catch(Exception e){
        	log.error("解密文件时发生错误："+e.toString());
        }
    }
    
    public List<String> getFiles(String dir){  
        List<String> lstFiles = null;       
        if(lstFiles == null){  
            lstFiles = new ArrayList<String>();  
        }  
        File file = new File(dir);
        if(!file.isDirectory()){
        	lstFiles.add(file.getAbsolutePath());
        }else{
	        File [] files = file.listFiles();  
	        for(File f : files){  
	            if(f.isDirectory()){  
	                lstFiles.add(f.getAbsolutePath());  
	                lstFiles.addAll(getFiles(f.getAbsolutePath()));  
	            }else{   
	                String str =f.getAbsolutePath();  
	                lstFiles.add(str);  
	            }  
	        } 
        }
        return lstFiles;  
    } 
    
    private boolean delFile(File file) {
		if (!file.exists()) {
			return false;
		}
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				delFile(f);
			}
		}
		return file.delete();
	}
    
    public static void main(String[] args) throws Exception {
        try {
        	SecureUtils su = new SecureUtils();
            String original = "200";
            System.out.println("Oringal: \"" + original + "\"");
            String enc = su.encrypt_DES3(original);
            System.out.println("Encrypted: \"" + enc + "\"");
            String dec = su.decrypt_DES3("6E7931E6C050BE8A");
            System.out.println("Decrypted: \"" + dec + "\"");
            if (dec.equals(original)) {
                System.out.println("Encryption ==> Decryption Successful");
            }
            String aesOriginal = "200";
            System.out.println("aesOriginal: \"" + aesOriginal + "\"");
            String aesenc = su.encrypt_AES(aesOriginal);
            System.out.println("AES_Encrypted: \"" + aesenc + "\"");
            String aesdec = su.decrypt_AES("2EC8C7097B431827DC9A258D7D53BEE8");
            System.out.println("AESDecrypted: \"" + aesdec + "\"");
            if (dec.equals(original)) {
                System.out.println("AESEncryption ==> AESDecryption Successful");
            }
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}

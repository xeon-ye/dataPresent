package com.tiger.dataPresent.utils;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.tiger.utils.BeanConfigUtils;

@Service("secureUtils")  
public class SecureUtils {
	private static Logger log = LoggerFactory.getLogger(SecureUtils.class);
	@Value("${KEY_DES3:37C50DD06617463FAF1FBF8E3B34DC23D46297E05E23D36C}")
	private String keyDES3;
	
	@Value("${KEY_AES:8F93E35572BA03E234FC74C9F75A074A}")
	private String keyAES;
	
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
        	iv = new IvParameterSpec(Hex.decodeHex("0123456789abcdef".toCharArray()));
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
        	iv = new IvParameterSpec(Hex.decodeHex("0123456789abcdef".toCharArray()));
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
            byte[] ivs = "0123456789abcdef".getBytes("UTF-8");
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
            byte[] ivs = "0123456789abcdef".getBytes("UTF-8");
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

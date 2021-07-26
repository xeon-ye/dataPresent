package com.tiger.utils.bean;

import com.alibaba.fastjson.JSONObject;

public class QuerySingleRdJson {
	private String dataID;
	private String keyParams ;
	public String getDataID() {
		return dataID;
	}
	public void setDataID(String dataID) {
		this.dataID = dataID;
	}
	public String getKeyParams() {
		return keyParams;
	}
	public void setKeyParams(String keyParams) {
		this.keyParams = keyParams;
	}
	public JSONObject parseJKeyParams(){
		return JSONObject.parseObject(this.keyParams);
	}
}

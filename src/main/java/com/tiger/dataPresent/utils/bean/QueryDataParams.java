package com.tiger.dataPresent.utils.bean;

import com.alibaba.fastjson.JSONObject;

public class QueryDataParams {
	private String dataID;
	private String depID;
	private String timestamp;
	private String signature;
	private String queryParams ;
	
	public String getDepID() {
		return depID;
	}
	public void setDepID(String depID) {
		this.depID = depID;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getSignature() {
		return signature;
	}
	public void setSignature(String signature) {
		this.signature = signature;
	}
	
	public String getDataID() {
		return dataID;
	}
	public void setDataID(String dataID) {
		this.dataID = dataID;
	}
	public String getQueryParams() {
		return queryParams;
	}
	public void setQueryParams(String queryParams) {
		this.queryParams = queryParams;
	}
	public JSONObject parseJQueryParams(){
		return JSONObject.parseObject(this.queryParams);
	}
}

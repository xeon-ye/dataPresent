package com.tiger.utils.bean;

import com.alibaba.fastjson.JSONObject;

public class GetDataJson {
	private String dataID;
	private String queryParams ;
	
	public JSONObject parseJQueryParams(){
		return JSONObject.parseObject(this.queryParams);
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
}

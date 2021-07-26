package com.tiger.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.util.StringUtils;

public class MultilDataSources {
	private List dataSources = new ArrayList();
	private Map dataSourcesMap = new HashMap();
	private List jdbcTemplates = new ArrayList();
	private Map jdbcTemplatesMap = new HashMap();
	private JdbcTemplate primaryJdbcTemplate=null;
	private String primaryDataSource;
	
	public void addDataSource(String dtName,DataSource dataSource){
		dataSources.add(dataSource);
		dataSourcesMap.put(dtName, dataSource);
	}
	public String getPrimaryDataSource(){
		return this.primaryDataSource;
	}
	public void setPrimaryDataSource(String primaryDataSource){
		this.primaryDataSource=primaryDataSource;
	}
	
	public JdbcTemplate getPrimaryJdbcTemplate(){
		return this.primaryJdbcTemplate;
	}
	public void setPrimaryJdbcTemplate(JdbcTemplate jdbcTemplate){
		this.primaryJdbcTemplate=jdbcTemplate;
	}
	public List getDataSouces(){
		return this.dataSources;
	}
	
	public Map getDataSoucesMap(){
		return this.dataSourcesMap;
	}
	
	public DataSource getDataSource(String dtName){
		if(StringUtils.isEmpty(dtName)){
			return (DataSource)this.dataSourcesMap.get(primaryDataSource);
		}
		return (DataSource)this.dataSourcesMap.get(dtName);
	}
	
	public void addJdbcTemplate(String dtName,JdbcTemplate jdbcTemplate){
		jdbcTemplates.add(jdbcTemplate);
		jdbcTemplatesMap.put(dtName, jdbcTemplate);
	}
	
	public List getJdbcTemplates(){
		return this.jdbcTemplates;
	}
	
	public Map getJdbcTemplatesMap(){
		return this.jdbcTemplatesMap;
	}
	
	public JdbcTemplate getJdbcTemplate(String dtName){
		if(StringUtils.isEmpty(dtName)){
			return (JdbcTemplate)this.jdbcTemplatesMap.get(primaryDataSource);
		}
		return (JdbcTemplate)this.jdbcTemplatesMap.get(dtName);
	}
}

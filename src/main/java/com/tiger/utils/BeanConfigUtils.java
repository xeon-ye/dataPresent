package com.tiger.utils;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.StringUtils;

@Configuration
public class BeanConfigUtils {
	private static Logger log = LoggerFactory.getLogger(BeanConfigUtils.class);
	
	@Value("${dataSourceNames}")
	private String dataSourceNames;
	
	@Value("${primaryDataSource}")
	private String primaryDataSource;
	
	@Autowired
    private Environment environment;
	
	@Bean(name="multilDataSources")
    public MultilDataSources multilDataSources(){
		String[] dsNames = null;
		MultilDataSources mdts = new MultilDataSources();
		
		System.out.println("dataSourceNames:"+dataSourceNames);
		if(dataSourceNames!=null){
			dsNames = dataSourceNames.split(",");
			if(StringUtils.isEmpty(primaryDataSource)){
				mdts.setPrimaryDataSource(dsNames[0]);
			}else{
				mdts.setPrimaryDataSource(primaryDataSource);
			}
			for(int i=0;i<dsNames.length;i++){
				String dn = dsNames[i];
				try{
					DruidDataSource dtSrc = dataSource(dn);
					System.out.println("DruidDataSource:"+dtSrc.getName());
					mdts.addDataSource(dn, dtSrc);
					JdbcTemplate jdbcTemplate = jdbcTemplate(dtSrc);
					if(dn.equals(mdts.getPrimaryDataSource())){
						mdts.setPrimaryJdbcTemplate(jdbcTemplate);
					}
					mdts.addJdbcTemplate(dn, jdbcTemplate);
				}catch(Exception e){
					log.error("创建数据连接池时发生错误："+e.toString());
				}
				log.debug(dsNames[i]+":"+dn);
			}
		}
		return mdts;
    }
    public DruidDataSource dataSource(String dn) throws SQLException { 
		DruidDataSource druidDataSource = new DruidDataSource(); 
		String url = environment.getProperty(dn+".db.url");
		String username = environment.getProperty(dn+".db.username");
		String password = environment.getProperty(dn+".db.password");
		String driverClassName = environment.getProperty(dn+".db.driverClassName");
		int initialSize = environment.getProperty(dn+".db.initialSize",int.class);
		int minIdle = environment.getProperty(dn+".db.minIdle",int.class);
		int maxActive = environment.getProperty(dn+".db.maxActive",int.class);
		long maxWait = environment.getProperty(dn+".db.maxWait",long.class);
		long timeBetweenEvictionRunsMillis = environment.getProperty(dn+".db.timeBetweenEvictionRunsMillis",long.class);
		long minEvictableIdleTimeMillis = environment.getProperty(dn+".db.minEvictableIdleTimeMillis",long.class);
		String validationQuery = environment.getProperty(dn+".db.validationQuery");
		boolean testWhileIdle = environment.getProperty(dn+".db.testWhileIdle",boolean.class);
		boolean testOnBorrow = environment.getProperty(dn+".db.testOnBorrow",boolean.class);
		boolean testOnReturn = environment.getProperty(dn+".db.testOnReturn",boolean.class);
		boolean poolPreparedStatements = environment.getProperty(dn+".db.poolPreparedStatements",boolean.class);
		int maxPoolPreparedStatementPerConnectionSize = environment.getProperty(dn+".db.maxPoolPreparedStatementPerConnectionSize",int.class);
		String filters = environment.getProperty(dn+".db.filters.commons-log.connection-logger-name");
		String connectionProperties = environment.getProperty(dn+".db.connectionProperties");
		druidDataSource.setDriverClassName(driverClassName); 
		druidDataSource.setUrl(url); 
		druidDataSource.setUsername(username); 
		druidDataSource.setPassword(password); 
		druidDataSource.setInitialSize(initialSize); 
		druidDataSource.setMinIdle(minIdle); 
      	druidDataSource.setMaxActive(maxActive); 
      	druidDataSource.setMaxWait(maxWait); 
      	druidDataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis); 
      	druidDataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis); 
      	druidDataSource.setValidationQuery(validationQuery); 
      	druidDataSource.setTestWhileIdle(testWhileIdle); 
      	druidDataSource.setTestOnBorrow(testOnBorrow); 
      	druidDataSource.setTestOnReturn(testOnReturn); 
      	druidDataSource.setPoolPreparedStatements(poolPreparedStatements); 
      	druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(maxPoolPreparedStatementPerConnectionSize); 
      	druidDataSource.setFilters(filters);
      	druidDataSource.setConnectionProperties(connectionProperties);
      	return druidDataSource; 
    }
	public JdbcTemplate jdbcTemplate(DataSource dsOne) {
        return new JdbcTemplate(dsOne);
    }
}

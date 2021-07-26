package com.tiger.dataPresent.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.mindrot.jbcrypt.BCrypt;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.alibaba.fastjson.JSONObject;
import com.tiger.dataPresent.utils.SecureUtils;
import com.tiger.dataPresent.utils.TemplatesLoader;
import com.tiger.dataPresent.utils.bean.QueryDataParams;
import com.tiger.dataPresent.utils.bean.template.Column;
import com.tiger.dataPresent.utils.bean.template.DataSrc;
import com.tiger.dataPresent.utils.bean.template.FilterField;
import com.tiger.dataPresent.utils.bean.template.JOutput;
import com.tiger.dataPresent.utils.bean.template.ProParaIn;
import com.tiger.dataPresent.utils.bean.template.ProParaOut;
import com.tiger.dataPresent.utils.bean.template.ProcedureBean;
import com.tiger.dataPresent.utils.bean.template.ValuedDs;
import com.tiger.dataPresent.utils.job.ExportJob;
import com.tiger.utils.JResponse;
import com.tiger.utils.JasyptUtils;
import com.tiger.utils.MultilDataSources;

@Service("publicDataService")  
@Transactional
public class PublicDataService {
	private static Logger log = LoggerFactory.getLogger(PublicDataService.class);
	@Autowired
	private MultilDataSources multilDataSources;
	@Autowired
	private TemplatesLoader templatesLoader;
	@Autowired
	private SecureUtils secureUtils;
	@Autowired
	private JasyptUtils jasyptUtils;
	@Autowired
    private Environment environment;
	@Autowired
	private Scheduler scheduler;
	
	public JSONObject fetchData(String dtID,String depID,JSONObject params){
		JSONObject result = new JSONObject();
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		if(ds==null){
			result.put("done", false);
			result.put("info", "未找到数据的定义信息。");
			log.equals("未找到数据的定义信息，ID："+dtID);
			return result;
		}
		try{
			JSONObject jdata =parseDtSrcOfRDB(dtID,params);
			
			//记录日志今后考虑移入多线程日志系统，比如kafka
			StringBuffer rSql = new StringBuffer("insert into etl_log(id, aid, depid, dtid, dotype, dotime)");
			rSql.append("values(sq_etl_id.nextval,0,?,?,0,sysdate)");
			Object[] lps=new Object[]{depID,dtID};
			JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
			jdbcTemplate.update(rSql.toString(),lps);
			result.put("done", true);
			result.put("data", jdata);
		}catch(Exception e){
			result.put("done", false);
			result.put("info", e.toString());
			log.equals(e.toString());
			return result;
		}
		return result;
	}
	
	//从关系型数据库查询数据
	public JSONObject parseDtSrcOfRDB(String dtID,JSONObject params)throws Exception{
		JSONObject info = new JSONObject();
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		String sSize = params.getString("size");
		int size = 1;
		try{
			size = Integer.parseInt(sSize);
		}catch(Exception e){
			size = 1;
		}
		int maxSize = ds.getMaxRowsPerFetch();
		//如果参数中的size大于数据源设置的每次取数上限，以上限为准，控制取数的量。
		if(maxSize<size){
			params.put("size", maxSize);
		}
		
		if(ds.getSourceType()==1){
			info = excuteSql(ds,params);
		}else if(ds.getSourceType()==2){
			info = excuteProcedure(ds,params);
		}
		return info;
	}
	@SuppressWarnings("unchecked")
	private JSONObject excuteProcedure(DataSrc ds,JSONObject paramVals)throws Exception{
		final JSONObject infos = new JSONObject();
		ProcedureBean pro=ds.getProcedure();
		if(pro==null){
			log.error("未设置取数存储过程！");
			throw new Exception("数据源定了存储过程取数方式，但未找到存储过程定义！");
		}
		
		List parasIn=pro.getInParas();
		StringBuffer proStmt=new StringBuffer("{call ");
		proStmt.append(pro.getName());
		//根据输入参数定义的个数设置?
		if(parasIn!=null&&parasIn.size()>0){
			proStmt.append("(");
			for(int i=0;i<parasIn.size();i++){
				proStmt.append("?");
				if(i<parasIn.size()-1){
					proStmt.append(",");
				}
			}
		}
		//根据输出参数定义继续设置?
		List parasOut=pro.getOutParas();
		if(parasOut!=null&&parasOut.size()>0){
			if(parasIn==null||parasIn.size()==0){
				proStmt.append("(");
			}else{
				proStmt.append(",");
			}
			for(int i=0;i<parasOut.size();i++){
				proStmt.append("?");
				if(i<parasOut.size()-1){
					proStmt.append(",");
				}else{
					proStmt.append(")");
				}
			}
		}else{
			if(parasIn!=null&&parasIn.size()>0){
				proStmt.append(")");
			}
		}
		proStmt.append("}");
		final List rows = new ArrayList();
		infos.put("total", 0);
		infos.put("rows", rows);
		Map decipherCols = ds.getDecipherColsMap();
		String dsId = ds.getId();
		String[] jtNames = jasyptUtils.parseDataPros(dsId);
		JdbcTemplate jdbcTemplate= multilDataSources.getJdbcTemplate(jtNames[0]);
		jdbcTemplate.execute(proStmt.toString(),new CallableStatementCallback(){
			public Object doInCallableStatement(CallableStatement cs)throws SQLException, DataAccessException {
				if(parasIn!=null&&parasIn.size()>0){
					for(int i=0;i<parasIn.size();i++){
						//过程参数引用方式分直接引用固定值和引用参数两种
						ProParaIn pi=(ProParaIn)parasIn.get(i);
						if(pi!=null&&pi.getReferMode()==0){
							cs.setString(i+1, pi.getValue());
						}else{
							if(paramVals==null){
								log.error("缺少参数值。参数："+pi.getReferTo());
							}
							String val=paramVals.getString(pi.getReferTo());
							cs.setString(i+1,val);
							log.info("参数(字符串)"+pi.getReferTo()+":"+val);
						}
					}
				}
				//注册输出参数
				int oStart=parasIn==null?1:parasIn.size()+1;
				if(parasOut!=null){
					for(int i=0;i<parasOut.size();i++){
						ProParaOut po=(ProParaOut)parasOut.get(i);
						if(po.getDataType()==1||po.getDataType()==2){
							cs.registerOutParameter(oStart+i, Types.NUMERIC);
						}else if(po.getDataType()==0){
							cs.registerOutParameter(oStart+i, Types.VARCHAR);
						}else if(po.getDataType()==3){
							cs.registerOutParameter(oStart+i, oracle.jdbc.OracleTypes.CURSOR);
						}
					}
				}
                cs.execute(); 
                int ti=ds.getProcedure().getTotalIndex();
            	int total = cs.getInt(oStart-1+ti);
                infos.put("total", total);
                ResultSet rs = (ResultSet)cs.getObject(oStart-1+pro.getDataSetIndex()); 
                if(rs==null){
                	return 0;
                }
                List cols = ds.getCols();
    			if(cols!=null){
    				while(rs.next()){
    		        	Map row = new HashMap();
    		        	for (int j=0;j<cols.size();j++){  
    			        	Column col = (Column)cols.get(j);
    		        		try{
    		        			String v = rs.getString(col.getName().toLowerCase());
    		        			//如果有需要解密的字段，解密该字段
    					    	if(col.getDecipher()==1){
    					    		String algorithm = col.getAlgorithm();
    					    		v = doDecipher(v,algorithm);
    					    	}
    		        			row.put(col.getName().toLowerCase(),v);
    		        		}catch(Exception e){
    		        		}
    			        }
    		        	rows.add(row);
    				}
    			}else{
	                ResultSetMetaData rsmd=rs.getMetaData();
	        		//获取元信息
	        		int colNum=rsmd.getColumnCount();
	                while (rs.next()) {
	                	Map row = new HashMap();
	                	for(int i=1;i<=colNum;i++){
	        				String colName = rsmd.getColumnLabel(i).toLowerCase();
			        		String v = rs.getString(i);
		        			//如果有需要解密的字段，解密该字段
					    	if(decipherCols!=null&&decipherCols.containsKey(colName.toLowerCase())){
					    		Column col = (Column)decipherCols.get(colName.toLowerCase());
					    		String algorithm = col.getAlgorithm();
					    		v = doDecipher(v,algorithm);
					    	}
					    	row.put(colName, v);
	        			}
	                	rows.add(row);
	                }
    			}
                infos.put("rows", rows);
                return infos;
			}
		});
		return infos;
	}
		
	private JSONObject excuteSql(DataSrc ds,JSONObject paramVals)throws Exception{
		int cc = 0;
		String sql=ds.getSql();
		if(sql==null)return null;
		JSONObject qinfos = new JSONObject();
		//先处理参数值替换
		String[] paras=StringUtils.substringsBetween(sql, "{", "}");
		
		String[] rpl2pers = StringUtils.substringsBetween(sql,"%[","]%");
		if(rpl2pers!=null&&rpl2pers.length>0){
			for(int i=0;i<rpl2pers.length;i++){
				sql = replaceParamValue(sql,rpl2pers[i],paramVals,2);
			}
		}
		String[] rplFpers=StringUtils.substringsBetween(sql,"%[","]");
		if(rplFpers!=null&&rplFpers.length>0){
			for(int i=0;i<rplFpers.length;i++){
				sql = replaceParamValue(sql,rplFpers[i],paramVals,0);
			}
		}
		String[] rplTpers=StringUtils.substringsBetween(sql,"[","]%");
		if(rplTpers!=null&&rplTpers.length>0){
			for(int i=0;i<rplTpers.length;i++){
				sql = replaceParamValue(sql,rplTpers[i],paramVals,1);
			}
		}
		String[] rplParas = StringUtils.substringsBetween(sql, "[", "]");
		if(rplParas!=null&&rplParas.length>0){
			for(int i=0;i<rplParas.length;i++){
				sql = replaceParamValue(sql,rplParas[i],paramVals,9);
			}
		}
		//如果有参数引用――{abc..}，替换成?，并提取其中的参数
		//2009-04-28为适应like中的%%
		String[] has2pers=StringUtils.substringsBetween(sql,"%{","}%");
		sql=sql.replaceAll("%\\{\\w*\\}%","?");
		String[] hasFpers=StringUtils.substringsBetween(sql,"%{","}");
		sql=sql.replaceAll("%\\{\\w*\\}","?");
		//String[] hasTpers=StringUtils.substringsBetween(sql,"{","}%");
		List lstTpers = new ArrayList();
		String tmpSql = sql;
		while(true){
			int end= tmpSql.indexOf("}%");
			if(end<0){
				break;
			}
			String preSql = tmpSql.substring(0,end);
			int start = preSql.lastIndexOf("{");
			String p = preSql.substring(start+1,end);
			tmpSql = tmpSql.substring(end+1);
			lstTpers.add(p);
		}
		String[] hasTpers =lstTpers.size()==0?null:new String[lstTpers.size()]; 
		for(int i=0;i<lstTpers.size();i++){
			hasTpers[i]=(String)lstTpers.get(i);
		}
		sql=sql.replaceAll("\\{\\w*\\}%","?");
		Map paraSearchModes=new HashMap();
		if(has2pers!=null){
			for(int i=0;i<has2pers.length;i++){
				paraSearchModes.put(has2pers[i], "2");
			}
		}
		if(hasFpers!=null){
			for(int i=0;i<hasFpers.length;i++){
				paraSearchModes.put(hasFpers[i], "0");
			}
		}
		if(hasTpers!=null){
			for(int i=0;i<hasTpers.length;i++){
				paraSearchModes.put(hasTpers[i], "1");
			}
		}
		sql=sql.replaceAll("\\{\\w*\\}","?");
		StringBuffer qSql = new StringBuffer("SELECT COUNT(*) AS RCOUNT FROM(");
        qSql.append(sql).append(")");
        //如果没有参数引用
        String dsId = ds.getId();
		String[] jtNames = jasyptUtils.parseDataPros(dsId);
		JdbcTemplate jdbcTemplate= multilDataSources.getJdbcTemplate(jtNames[0]);
  		if(paras==null||paras.length==0){
  			cc = jdbcTemplate.queryForObject(qSql.toString(),Integer.class);
  		}else{
  			Object[] params = parseSqlParameter(paramVals,paras,paraSearchModes);
  			cc = jdbcTemplate.queryForObject(qSql.toString(),params,Integer.class);
  		}
	    qinfos.put("total", cc);
		//如果分页取，加工分页sql
		String sFrom = paramVals.getString("from");
		String sSize = paramVals.getString("size");
		int from = 0, size= 10;
		try{
			from = Integer.parseInt(sFrom);
		}catch(Exception e){}
		try{
			size = Integer.parseInt(sSize);
		}catch(Exception e){
			size=10;
		}
		qSql = new StringBuffer("SELECT * FROM (SELECT A.*, rownum r FROM (");
        qSql.append(sql);
        qSql.append(") A WHERE rownum<=");
        qSql.append((from+size));
        qSql.append(") B WHERE r>");
        qSql.append(from);
        sql = qSql.toString();
		
		List lst = null;
		//如果没有参数引用
		jdbcTemplate= multilDataSources.getJdbcTemplate(jtNames[0]);
		if(paras==null||paras.length==0){
			lst = jdbcTemplate.queryForList(qSql.toString());
		}else{
			Object[] params = parseSqlParameter(paramVals,paras,paraSearchModes);
			lst = jdbcTemplate.queryForList(qSql.toString(),params);
		}
		Map decipherCols = ds.getDecipherColsMap();
		if(lst!=null&&lst.size()>0){
			List rows = new ArrayList();
			List cols = ds.getCols();
			if(cols!=null){
				for(int i=0;i<lst.size();i++){
					Map row = (Map)lst.get(i);
		        	Map nrow = new HashMap();
		        	for (int j=0;j<cols.size();j++){ 
		        		Column col = (Column)cols.get(j);
		        		if(row.containsKey(col.getName().toLowerCase())){
		        			String v = row.get(col.getName().toLowerCase())==null?"":row.get(col.getName().toLowerCase()).toString();
		        			//如果有需要解密的字段，解密该字段
					    	if(col.getDecipher()==1){
					    		String algorithm = col.getAlgorithm();
					    		v = doDecipher(v,algorithm);
					    	}
		        			nrow.put(col.getName().toLowerCase(),v); 
		        		}
			        }
		        	rows.add(nrow);
				}
			}else{
		        for(int i=0;i<lst.size();i++){
		        	Map row = (Map)lst.get(i);
		        	Map nrow = new HashMap();
		        	for (Iterator it = row.keySet().iterator(); it.hasNext();) {  
		        		String key = (String)it.next();
		        		String v = row.get(key)==null?"":row.get(key).toString();
	        			//如果有需要解密的字段，解密该字段
				    	if(decipherCols!=null&&decipherCols.containsKey(key.toLowerCase())){
				    		Column col = (Column)decipherCols.get(key.toLowerCase());
				    		String algorithm = col.getAlgorithm();
				    		v = doDecipher(v,algorithm);
				    	}
		        		nrow.put(key.toLowerCase(), v); 
			        }
		        	rows.add(nrow);
		        }
			}
	        qinfos.put("rows", rows);
		}
		return qinfos;
	}
	private String replaceParamValue(String sql,String pName,JSONObject paraValues,int rmode){
		String rvalue = (String)paraValues.get(pName);
		if(rmode==9){//如果不是like，('%%')的形式，需转化成每个逗号之间都插入单引号
			rvalue = rvalue.replaceAll(",", "','");
		}
		//值的替换，有like操作的，一律当字符串处理，加''，非like操作，要根据数据类型确定是否添加''
		if(rmode==2){//%在两头
			sql=sql.replace("%["+pName+"]%","'%"+rvalue+"%'");
		}else if(rmode==0){//%在前
			sql=sql.replace("%["+pName+"]","'%"+rvalue+"'");
		}else if(rmode==1){//%在后
			sql=sql.replace("["+pName+"]%","'"+rvalue+"%'");
		}else{//无%
			sql=sql.replace("["+pName+"]","'"+rvalue+"'");
		}
		return sql;
	}
	private Object[] parseSqlParameter(JSONObject paraValues,String[] paras,Map fuzzySearchPara)throws Exception{
		//如果没有引用参数，可以直接返回
		if(paras==null||paras.length==0)return null;
		Object[] pvs = new Object[paras.length];
		for(int i=0;i<paras.length;i++){
			String val="";
			Object oval=paraValues.get(paras[i]);
			if(oval==null){
				val=null;
			}else{
				val = String.valueOf(oval);
			}
			if(fuzzySearchPara.containsKey(paras[i])){
				if("2".equals(fuzzySearchPara.get(paras[i]))){
					val="%"+val+"%";
				}else if("0".equals(fuzzySearchPara.get(paras[i]))){
					val="%"+val;
				}else if("1".equals(fuzzySearchPara.get(paras[i]))){
					val=val+"%";
				}
			}
			pvs[i]=val;
			log.debug("参数"+paras[i]+":"+val);
		}
		return pvs;
	}

	public JSONObject updateDepSecret(String depID) {
		JSONObject result = new JSONObject();
		String uuid = UUID.randomUUID().toString().replaceAll("-","");
		//根据uuid，hash一遍才是真正的secret
		String hashed = BCrypt.hashpw(uuid, BCrypt.gensalt());
		//加密再保存
		String secret = jasyptUtils.encypt(hashed,"");
		String sql = "update ETL_DEP set depsecret=? where id=?";
		JdbcTemplate jt= multilDataSources.getPrimaryJdbcTemplate();
		jt.update(sql,new Object[]{secret,depID});
		result.put("done", true);
		return result;
	}

	public JSONObject updateDepAESkey(String depID) {
		JSONObject result = new JSONObject();
		//生成128位AES的key
		String key = secureUtils.createAESKey(128);
		//加密后存放
		String aeskey = jasyptUtils.encypt(key,"");
		String sql = "update ETL_DEP set dtkey=? where id=?";
		JdbcTemplate jt= multilDataSources.getPrimaryJdbcTemplate();
		jt.update(sql,new Object[]{aeskey,depID});
		result.put("done", true);
		return result;
	}

	public JSONObject applyForExtractData(String dtID,String depID,JSONObject params) {
		JSONObject result = new JSONObject();
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		if(ds==null){
			result.put("done", false);
			result.put("info", "未找到数据的定义信息。");
			log.equals("未找到数据的定义信息，ID："+dtID);
			return result;
		}
		try{
			JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
			int newid = jdbcTemplate.queryForObject("select sq_etl_apply_id.nextval from dual", Integer.class);
			
			StringBuffer sql = new StringBuffer("insert into etl_apply(id,depid,dtid,ismultiple,expformat,timer,paramvals,remark,approved)");
			sql.append("values(?,?,?,?,?,?,?,?,0)");
			String ismultiple = params.get("ismultiple")==null?"0":params.get("ismultiple").toString();
			String expformat= params.get("expformat")==null?"0":params.get("expformat").toString();
			String timer=params.get("timer")==null?"":params.get("timer").toString();
			String paramvals=params.get("paramvals")==null?"{}":params.get("paramvals").toString();
			String remark=params.get("remark")==null?"":params.get("remark").toString();
			jdbcTemplate.update(sql.toString(),newid,depID,dtID,ismultiple,expformat,timer,paramvals,remark);
			result.put("done", true);
			result.put("applyID", newid);
		}catch(Exception e){
			result.put("done", false);
			result.put("info", e.toString());
			log.equals(e.toString());
			return result;
		}
		return result;
	}
	
	private String doDecipher(String rawValue,String algorithm){
		String newVal = secureUtils.decipher(rawValue,algorithm);
		return newVal;
	}
	
	public JResponse preAuth(QueryDataParams qb){
		JResponse jr = null;
		String depID="",timestamp="",signature="";
		depID = qb.getDepID();
		timestamp = qb.getTimestamp();
		signature = qb.getSignature();
		if(StringUtils.isEmpty(depID)){
			jr = new JResponse("9","未提供depID参数！",null);
			return jr;
		}
		if(StringUtils.isEmpty(timestamp)){
			jr = new JResponse("9","未提供timestamp参数！",null);
			return jr;
		}
		if(StringUtils.isEmpty(signature)){
			jr = new JResponse("9","未提供signature参数！",null);
			return jr;
		}
		//验证签名
		boolean valid = authenticate(depID,timestamp,signature);
		
		if(!valid){
			jr = new JResponse("9","身份验证未通过！",null);
		}else{
			jr = new JResponse("0","身份验证通过！",null);
		}
		return jr;
	}
	
	public boolean authenticate(String depID, String timestamp, String signature) {
		boolean consist = false;
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		String secret = jdbcTemplate.queryForObject("select nvl(depsecret,'')depsecret from etl_dep where id = ?",String.class,depID);
		String key = jasyptUtils.decypt(secret, null);
		String plaintext = depID+key+timestamp;
		try{
			consist = BCrypt.checkpw(plaintext, signature);
		}catch(Exception e){
			log.error("验证密码时发生错误，验证串："+plaintext+"，错误信息："+e.toString());
		}
		return consist;
	}

	public String encryptAES(String depID,JSONObject data) {
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		String dtAESkey = jdbcTemplate.queryForObject("select dtkey from etl_dep where id=?",String.class,depID);
		String AESkey = jasyptUtils.decypt(dtAESkey, null);
		String jstr = data.toJSONString();
		String encrypt = secureUtils.encrypt_AES(AESkey,jstr);
		return encrypt;
	}

	public String decryptAES(String depID,String sData) {
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		String dtAESkey = jdbcTemplate.queryForObject("select dtkey from etl_dep where id=?",String.class,depID);
		String AESkey = jasyptUtils.decypt(dtAESkey, null);
		String decrypt = secureUtils.decrypt_AES(AESkey,sData);
		return decrypt;
	}

	public JSONObject decryptFileAES(String depID,String filename) {
		JSONObject result = new JSONObject();
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		String dtAESkey = jdbcTemplate.queryForObject("select dtkey from etl_dep where id=?",String.class,depID);
		String AESkey = jasyptUtils.decypt(dtAESkey, null);
		try{
			secureUtils.fileDecodeAES(AESkey, filename);
			result.put("done", true);
		}catch(Exception e){
			result.put("done", false);
			result.put("info", e.toString());
		}
		return result;
	}

	public JSONObject queryExtractInfo(JSONObject params) {
		JSONObject qinfos = new JSONObject();
		try{
			String aid = params.get("applyID").toString();
			String sql = "select id, fsize from etl_result where aid=? order by id desc";
			StringBuffer qSql = new StringBuffer("select count(*) as rcount from(");
			qSql.append(sql).append(")");
			JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
	  		int cc = jdbcTemplate.queryForObject(qSql.toString(),Integer.class,aid);
		    qinfos.put("total", cc);
			//如果分页取，加工分页sql
			String sFrom = params.getString("from");
			String sSize = params.getString("size");
			int from = 0, size= 10;
			try{
				from = Integer.parseInt(sFrom);
			}catch(Exception e){}
			try{
				size = Integer.parseInt(sSize);
			}catch(Exception e){
				size=10;
			}
			qSql = new StringBuffer("SELECT * FROM (SELECT A.*, rownum r FROM (");
	        qSql.append(sql);
	        qSql.append(") A WHERE rownum<=");
	        qSql.append((from+size));
	        qSql.append(") B WHERE r>");
	        qSql.append(from);
	        sql = qSql.toString();
	        List lst = jdbcTemplate.queryForList(qSql.toString(),aid);
	        if(lst!=null&&lst.size()>0){
	        	List rows = new ArrayList();
		        for(int i=0;i<lst.size();i++){
		        	Map row = (Map)lst.get(i);
		        	Map nrow = new HashMap();
		        	for (Iterator it = row.keySet().iterator(); it.hasNext();) {  
		        		String key = (String)it.next();
		        		String v = row.get(key)==null?"":row.get(key).toString();
		        		nrow.put(key.toLowerCase(), v); 
			        }
		        	rows.add(nrow);
		        }
		        qinfos.put("rows", rows);
			}
		}catch(Throwable e){
			log.error(e.toString());
			qinfos.put("info", e.getMessage());
		}
		return qinfos;
	}

	public void exportListData(HttpServletResponse response, String resultID) throws Exception{
		ServletOutputStream out = null;
        FileInputStream in = null;
        try{
        	String zipPath = "",fileName = "";
        	String sql = "select fpath from etl_result where id=? ";
    		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
    		zipPath = jdbcTemplate.queryForObject(sql,String.class,resultID);
    		if(StringUtils.isEmpty(zipPath)){
    			throw new Exception("未找到resultID为"+resultID+"的导出文件！");
    		}
    		fileName = zipPath.substring(zipPath.lastIndexOf("/")+1,zipPath.length());
			in = new FileInputStream(new File(zipPath));
	        //设置响应头，控制浏览器下载该文件
	        response.setHeader("Content-Type","application/zip");
	        response.setHeader("Content-Disposition",
	                "attachment;filename="+java.net.URLEncoder.encode(fileName, "UTF-8"));
	        out = response.getOutputStream();
	        int read = 0;
	        byte[] buffer = new byte[1024];
	        while((read = in.read(buffer)) != -1){
	            out.write(buffer, 0, read);
	        }
	        in.close();
	        out.flush();
        } finally {
	        try {
	        	in.close();
		        out.close();
	        }catch (Exception e){
	            e.printStackTrace();
	        }
	    }
	}

	public JSONObject approveExtractApply(String applyID) {
		JSONObject result = new JSONObject();
		try{
			JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
			List lst = jdbcTemplate.queryForList("select nvl(ismultiple,0)ismultiple,nvl(timer,'')timer from etl_apply where id=?",applyID);
			if(lst==null||lst.size()==0){
				result.put("done", false);
				result.put("info", "未找到指定申请ID的记录，申请ID："+applyID);
				return result;
			}
			Map appInfo = (Map)lst.get(0);
			int ismultiple = ((BigDecimal)appInfo.get("ismultiple")).intValue();
			String timer = (String)appInfo.get("timer");
			if(ismultiple == 1){
				jdbcTemplate.update("update etl_apply set approved=1, apptime = sysdate where id = ?",applyID);
			}else{
				//根据当前时间设置一次性的定时器
				Calendar cal = Calendar.getInstance();   
		        cal.add(Calendar.MINUTE, 1);// 24小时制   
		        Date date = cal.getTime();   
				SimpleDateFormat sdf = new SimpleDateFormat("ss mm HH dd MM ? yyyy");
				timer = sdf.format(date);
				jdbcTemplate.update("update etl_apply set approved=1, apptime = sysdate,timer=? where id = ?",timer,applyID);
			}
			//添加调度任务
			JobDetail jcDt = JobBuilder.newJob(ExportJob.class)
				      .withIdentity("job_export_"+applyID, "export")
				      .build();
			jcDt.getJobDataMap().put("aid", applyID);
			Trigger trigger = null;
			try{
				trigger = TriggerBuilder.newTrigger().withIdentity("tg_export_"+applyID, "export")
					.withSchedule(CronScheduleBuilder.cronSchedule(timer)).build(); 
			}catch(Exception e){
				log.error("触发器时间配置格式错误，任务类型：导出数据"+",申请id："+applyID+",时间表达式："+timer);
			}
			try{
				scheduler.scheduleJob(jcDt, trigger);
			}catch(Exception e){
				log.error("加入调度任务失败"+",申请id："+applyID+",时间表达式："+timer);
			}
			result.put("done", true);
		}catch(Exception e){
			result.put("done", false);
			result.put("info", e.toString());
			log.equals(e.toString());
			return result;
		}
		return result;
	}
}

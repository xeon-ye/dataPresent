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
			result.put("info", "?????????????????????????????????");
			log.equals("?????????????????????????????????ID???"+dtID);
			return result;
		}
		try{
			JSONObject jdata =parseDtSrcOfRDB(dtID,params);
			
			//????????????????????????????????????????????????????????????kafka
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
	
	//?????????????????????????????????
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
		//??????????????????size????????????????????????????????????????????????????????????????????????????????????
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
			log.error("??????????????????????????????");
			throw new Exception("???????????????????????????????????????????????????????????????????????????");
		}
		
		List parasIn=pro.getInParas();
		StringBuffer proStmt=new StringBuffer("{call ");
		proStmt.append(pro.getName());
		//????????????????????????????????????????
		if(parasIn!=null&&parasIn.size()>0){
			proStmt.append("(");
			for(int i=0;i<parasIn.size();i++){
				proStmt.append("?");
				if(i<parasIn.size()-1){
					proStmt.append(",");
				}
			}
		}
		//?????????????????????????????????????
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
						//?????????????????????????????????????????????????????????????????????
						ProParaIn pi=(ProParaIn)parasIn.get(i);
						if(pi!=null&&pi.getReferMode()==0){
							cs.setString(i+1, pi.getValue());
						}else{
							if(paramVals==null){
								log.error("???????????????????????????"+pi.getReferTo());
							}
							String val=paramVals.getString(pi.getReferTo());
							cs.setString(i+1,val);
							log.info("??????(?????????)"+pi.getReferTo()+":"+val);
						}
					}
				}
				//??????????????????
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
    		        			//????????????????????????????????????????????????
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
	        		//???????????????
	        		int colNum=rsmd.getColumnCount();
	                while (rs.next()) {
	                	Map row = new HashMap();
	                	for(int i=1;i<=colNum;i++){
	        				String colName = rsmd.getColumnLabel(i).toLowerCase();
			        		String v = rs.getString(i);
		        			//????????????????????????????????????????????????
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
		//????????????????????????
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
		//???????????????????????????{abc..}????????????????????????????????????????
		//2009-04-28?????????like??????%%
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
        //????????????????????????
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
		//??????????????????????????????sql
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
		//????????????????????????
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
		        			//????????????????????????????????????????????????
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
	        			//????????????????????????????????????????????????
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
		if(rmode==9){//????????????like???('%%')????????????????????????????????????????????????????????????
			rvalue = rvalue.replaceAll(",", "','");
		}
		//??????????????????like??????????????????????????????????????????''??????like????????????????????????????????????????????????''
		if(rmode==2){//%?????????
			sql=sql.replace("%["+pName+"]%","'%"+rvalue+"%'");
		}else if(rmode==0){//%??????
			sql=sql.replace("%["+pName+"]","'%"+rvalue+"'");
		}else if(rmode==1){//%??????
			sql=sql.replace("["+pName+"]%","'"+rvalue+"%'");
		}else{//???%
			sql=sql.replace("["+pName+"]","'"+rvalue+"'");
		}
		return sql;
	}
	private Object[] parseSqlParameter(JSONObject paraValues,String[] paras,Map fuzzySearchPara)throws Exception{
		//?????????????????????????????????????????????
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
			log.debug("??????"+paras[i]+":"+val);
		}
		return pvs;
	}

	public JSONObject updateDepSecret(String depID) {
		JSONObject result = new JSONObject();
		String uuid = UUID.randomUUID().toString().replaceAll("-","");
		//??????uuid???hash?????????????????????secret
		String hashed = BCrypt.hashpw(uuid, BCrypt.gensalt());
		//???????????????
		String secret = jasyptUtils.encypt(hashed,"");
		String sql = "update ETL_DEP set depsecret=? where id=?";
		JdbcTemplate jt= multilDataSources.getPrimaryJdbcTemplate();
		jt.update(sql,new Object[]{secret,depID});
		result.put("done", true);
		return result;
	}

	public JSONObject updateDepAESkey(String depID) {
		JSONObject result = new JSONObject();
		//??????128???AES???key
		String key = secureUtils.createAESKey(128);
		//???????????????
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
			result.put("info", "?????????????????????????????????");
			log.equals("?????????????????????????????????ID???"+dtID);
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
			jr = new JResponse("9","?????????depID?????????",null);
			return jr;
		}
		if(StringUtils.isEmpty(timestamp)){
			jr = new JResponse("9","?????????timestamp?????????",null);
			return jr;
		}
		if(StringUtils.isEmpty(signature)){
			jr = new JResponse("9","?????????signature?????????",null);
			return jr;
		}
		//????????????
		boolean valid = authenticate(depID,timestamp,signature);
		
		if(!valid){
			jr = new JResponse("9","????????????????????????",null);
		}else{
			jr = new JResponse("0","?????????????????????",null);
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
			log.error("??????????????????????????????????????????"+plaintext+"??????????????????"+e.toString());
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
			//??????????????????????????????sql
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
    			throw new Exception("?????????resultID???"+resultID+"??????????????????");
    		}
    		fileName = zipPath.substring(zipPath.lastIndexOf("/")+1,zipPath.length());
			in = new FileInputStream(new File(zipPath));
	        //????????????????????????????????????????????????
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
				result.put("info", "?????????????????????ID??????????????????ID???"+applyID);
				return result;
			}
			Map appInfo = (Map)lst.get(0);
			int ismultiple = ((BigDecimal)appInfo.get("ismultiple")).intValue();
			String timer = (String)appInfo.get("timer");
			if(ismultiple == 1){
				jdbcTemplate.update("update etl_apply set approved=1, apptime = sysdate where id = ?",applyID);
			}else{
				//?????????????????????????????????????????????
				Calendar cal = Calendar.getInstance();   
		        cal.add(Calendar.MINUTE, 1);// 24?????????   
		        Date date = cal.getTime();   
				SimpleDateFormat sdf = new SimpleDateFormat("ss mm HH dd MM ? yyyy");
				timer = sdf.format(date);
				jdbcTemplate.update("update etl_apply set approved=1, apptime = sysdate,timer=? where id = ?",timer,applyID);
			}
			//??????????????????
			JobDetail jcDt = JobBuilder.newJob(ExportJob.class)
				      .withIdentity("job_export_"+applyID, "export")
				      .build();
			jcDt.getJobDataMap().put("aid", applyID);
			Trigger trigger = null;
			try{
				trigger = TriggerBuilder.newTrigger().withIdentity("tg_export_"+applyID, "export")
					.withSchedule(CronScheduleBuilder.cronSchedule(timer)).build(); 
			}catch(Exception e){
				log.error("???????????????????????????????????????????????????????????????"+",??????id???"+applyID+",??????????????????"+timer);
			}
			try{
				scheduler.scheduleJob(jcDt, trigger);
			}catch(Exception e){
				log.error("????????????????????????"+",??????id???"+applyID+",??????????????????"+timer);
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

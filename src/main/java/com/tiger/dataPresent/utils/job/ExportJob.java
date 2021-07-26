package com.tiger.dataPresent.utils.job;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSONObject;
import com.tiger.dataPresent.service.PublicDataService;
import com.tiger.dataPresent.utils.SecureUtils;
import com.tiger.dataPresent.utils.TemplatesLoader;
import com.tiger.dataPresent.utils.ZipService;
import com.tiger.dataPresent.utils.bean.template.Column;
import com.tiger.dataPresent.utils.bean.template.DataSrc;
import com.tiger.utils.JasyptUtils;
import com.tiger.utils.MultilDataSources;

public class ExportJob implements Job{
	private static Logger log = LoggerFactory.getLogger(ExportJob.class);
	@Autowired
	private MultilDataSources multilDataSources;
	@Autowired
	private PublicDataService publicDataService;
	@Autowired
    private Environment environment;
	@Autowired
	private TemplatesLoader templatesLoader;
	@Autowired
	private JasyptUtils jasyptUtils;
	@Autowired
	private SecureUtils secureUtils;
	@Autowired
	private ZipService zipService;
	
	public void execute(JobExecutionContext context)throws JobExecutionException{
		JobDetail jdt = context.getJobDetail();
		String aid =jdt.getJobDataMap().getString("aid");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String st = sdf.format(new Date());
		log.info("执行申请："+aid+",时间："+sdf.format(new Date()));
		String root = environment.getProperty("exportRoot","d:/exportRoot/");
		//获取取数的任务定义
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		StringBuffer sql = new StringBuffer("select to_char(a.id) aid,a.depid,d.dtkey,a.dtid,a.paramvals,e.rowsperfile,e.fileencode,");
		sql.append("e.colseperator,to_char(nvl(a.expformat,0))expformat from etl_apply a,etl_dep d,etl_interface e ");
		sql.append(" where a.depid=d.id and a.dtid=e.id and a.id=?");
		List extcfg = jdbcTemplate.queryForList(sql.toString(),aid);
		if(extcfg==null||extcfg.size()==0){
			log.error("任务中止，未找到任务的配置信息，申请执行的ID："+aid);
			return;
		}
		Map cfg = (Map)extcfg.get(0);
		String dtID = (String)cfg.get("dtid");
		String depID = (String)cfg.get("depid");
		
		String dtAESkey = (String) cfg.get("dtkey");
		String AESkey = jasyptUtils.decypt(dtAESkey, null);
		String encrypt = environment.getProperty("exportDataEncrypt","1");
		
		String paramvals = (String)cfg.get("paramvals");
		JSONObject jpvals = JSONObject.parseObject(paramvals);
		String sRowspf = cfg.get("rowsperfile").toString();
		int rowsPerFile = 100;
		try{
			rowsPerFile = Integer.parseInt(sRowspf);
		}catch(Exception e){}
		String fencode = (String)cfg.get("fileencode");
		String colsep = (String)cfg.get("colseperator");
		String expformat = cfg.get("expformat").toString();
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		if(ds==null){
			log.error("任务中止，未找到数据源的定义信息，申请执行的ID："+aid+",数据源ID："+dtID);
			log.equals("未找到数据的定义信息，ID："+dtID);
			return;
		}
		if(jpvals==null){
			jpvals = new JSONObject();
		}
		try{
			int extCounter = 0,start=0 ,size = ds.getMaxRowsPerFetch();
			//首次取数，先添加分页参数，每次取数不能超过数据源定义的单次阈值
			jpvals.put("from", start);
			jpvals.put("size", size);
			JSONObject jdata =publicDataService.parseDtSrcOfRDB(dtID,jpvals);
			if(jdata==null){
				log.error("取数未成功。申请执行的ID："+aid);
				return;
			}
			int total = jdata.getIntValue("total");
			//构造文件输出器
			FileWriter fw = new FileWriter();
			fw.setRecordCount(total);
			fw.setColSeparator(StringUtils.isEmpty(colsep)?",":colsep);
			fw.setEncode(fencode);
			fw.setRowsPerFile(rowsPerFile);
			root = (root.endsWith("/")? root : root + "/")+depID+"/";
			fw.setRootDir(root);
			fw.setSubDir(st);
			String fnameRoot = dtID;
			fw.setFileNameRoot(fnameRoot);
			
			List rows = jdata.getJSONArray("rows");
			//获取表头信息
			String[] colNames = getColumns(dtID,rows);
			fw.setSaveColumns(colNames);
			//先输出第一次取数的内容
			int outRows = outPutData(dtID,rows,fw,expformat);
			extCounter +=outRows;	
			while(extCounter<total){
		        jpvals.put("from", start+size);
				jpvals.put("size", size);
				jdata =publicDataService.parseDtSrcOfRDB(dtID,jpvals);
				if(jdata==null||!jdata.containsKey("rows")){
					break;
				}
				rows = jdata.getJSONArray("rows");
				if(rows==null||rows.size()==0){
					break;
				}
				outRows = outPutData(dtID,rows,fw,expformat);
				start = start+size;
				extCounter +=outRows;
			}
			if("0".equals(expformat)){
				fw.finishWriteFile();
			}
			String filesPath = root+st+"/";
			//加密
			if("1".equals(encrypt)){
				try{
					secureUtils.fileEncodeAES(AESkey,filesPath);
				}catch(Exception e){
					log.error("加密文件时发生错误："+e.toString());
				}
			}
			//压缩
			String zipFileName = root+dtID+"_"+st+".zip";
			compressFiles(filesPath, zipFileName);
			File zipFile = new File(zipFileName); 
			long sizeByte = zipFile.length();
			//记录导出结果
			StringBuffer rSql = new StringBuffer("insert into etl_result(id,aid,depid,dtid,dotime,fid,fpath,fsize,ftype)");
			rSql.append("values(sq_etl_id.nextval,?,?,?,sysdate,sq_file_id.nextval,?,?,?)");
			Object[] params=new Object[]{aid,depID,dtID,zipFileName,sizeByte,"zip"};
			jdbcTemplate.update(rSql.toString(),params);
			rSql = new StringBuffer("insert into etl_log(id, aid, depid, dtid, dotype, dotime)");
			rSql.append("values(sq_etl_id.nextval,?,?,?,1,sysdate)");
			params=new Object[]{aid,depID,dtID};
			jdbcTemplate.update(rSql.toString(),params);
			log.info(dtID+"取数完成！");
		}catch(Exception e){
			log.error("取数时发生错误，"+e.toString());
		}
	}
	private String[] getColumns(String dtID,List rows){
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		List cols = ds.getCols();
		List<String> outputCols = new ArrayList();
		if(cols!=null){
			for(int i=0;i<cols.size();i++){
				Column col = (Column)cols.get(i);
				String name = col.getName().toLowerCase();
				outputCols.add(name);
			}
		}else{
			Map row = (Map)rows.get(0);
			for (Iterator it = row.keySet().iterator(); it.hasNext();){  
				String key = (String)it.next();
				outputCols.add(key);
			}
		}
		String[] strCols = new String[outputCols.size()];
		strCols = outputCols.toArray(strCols);
		return strCols;
	}
	private int outPutData(String dtID,List rows,FileWriter fw,String format){
		List dataList = new ArrayList();
		DataSrc ds = templatesLoader.getDataSrc(dtID);
		List cols = ds.getCols();
		//如果有列定义
		if(cols!=null){
			for(int i=0;i<rows.size();i++){
				Map row = (Map)rows.get(i);
				List nrow = new ArrayList();
				for(int j=0;j<cols.size();j++){
					Column col = (Column)cols.get(j);
					String key = col.getName().toLowerCase();
	        		if(!row.containsKey(key)){
	        			nrow.add("");
	        			continue;
	        		}
					String v = row.get(key)==null?"":row.get(key).toString();
					nrow.add(v);
				}
				dataList.add(nrow);
			}
		}else{
			for(int i=0;i<rows.size();i++){
				Map row = (Map)rows.get(i);
				List nrow = new ArrayList();
				for (Iterator it = row.keySet().iterator(); it.hasNext();){  
					String key = (String)it.next();
	        		String v = row.get(key)==null?"":row.get(key).toString();
					nrow.add(v);
				}
				dataList.add(nrow);
			}
		}
		try{
			if("0".equals(format)){
				fw.writeTxt(dtID,dataList);
			}else{
				fw.writeExcel(dtID,dataList,format);
			}
		}catch(Exception e){
			log.error(e.toString());
		}
		return dataList.size();
	}
	public void encryptFiles(String AESkey,String filesPath){
		java.io.File dir=new java.io.File(filesPath);
		File[] fs = dir.listFiles(); 
		if(fs==null||fs.length==0)return;
		for(int i=0; i<fs.length; i++){ 
			//读取文件并输出加密文件
			File f = fs[i];
			String filename = f.getAbsolutePath();
			try{
				secureUtils.fileEncodeAES(AESkey,filename);
			}catch(Exception e){
				log.error("加密文件时发生错误："+e.toString());
			}
		}
		
	}
	public void compressFiles(String filesPath,String zipFileName){
		//压缩
		zipService.zip(filesPath, zipFileName);
		//删除被压缩的文件
		java.io.File dir=new java.io.File(filesPath);
	    delFiles(dir);
	}
	
	private boolean delFiles(File file) {
		if (!file.exists()) {
			return false;
		}
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f : files) {
				delFiles(f);
			}
		}
		return file.delete();
	}
}

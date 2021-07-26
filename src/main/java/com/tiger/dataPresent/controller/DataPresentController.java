package com.tiger.dataPresent.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tiger.dataPresent.service.DataPresentService;
import com.tiger.dataPresent.service.PublicDataService;
import com.tiger.dataPresent.utils.TemplatesLoader;
import com.tiger.dataPresent.utils.bean.QueryDataParams;
import com.tiger.dataPresent.utils.bean.RptDataJson;
import com.tiger.utils.JResponse;
import com.tiger.utils.JasyptUtils;
import com.tiger.utils.bean.GetDataJson;
import com.tiger.utils.bean.QuerySingleRdJson;

@CrossOrigin
@RestController
public class DataPresentController {
	private static Logger log = LoggerFactory.getLogger(DataPresentController.class);
	@Autowired
	private DataPresentService dataPresentService;
	@Autowired
	private PublicDataService publicDataService;
	@Autowired
	private TemplatesLoader templatesLoader;
	@Autowired
	private JasyptUtils jasyptUtils;
	@Autowired
    private ContextRefresher contextRefresher;
	@Autowired
    private Environment environment;
	
	@RequestMapping(value="/queryData",method = RequestMethod.POST)
	@ResponseBody
	public JResponse queryData(@RequestBody RptDataJson params){
		JResponse jr = null;
		if(params!=null){
			String rptID = params.getRptID();
			JSONObject qParams = params.parseJRptParams();
			Map data = null;
			try{
				data = dataPresentService.getData(rptID,qParams);
			}catch(Exception e){
				log.info("queryData异常。rptID："+rptID+"异常:"+e.toString());
				jr = new JResponse("9","查询数据时发生异常，未能查找到数据。",null);
				return jr;
			}
			if(data!=null&&data.containsKey("done")){
				boolean done = (Boolean)data.get("done");
				if(done){
					JSONObject jdata = (JSONObject)data.get("jpData");
					jr = new JResponse("0","",jdata);
				}else{
					String info = (String)data.get("info");
					jr = new JResponse("9",info,null);
				}
			}else{
				jr = new JResponse("9","获取页面数据失败！",null);
			}
			log.info(rptID+"的输出:"+jr.toString());
		}else{
			jr = new JResponse("9","获取报表数据失败，没有获得正确的请求参数！",null);
		}
		return jr;
	}
	
	@RequestMapping(value="/public/getData",method = RequestMethod.POST)
	@ResponseBody
	public JResponse getData(@RequestBody QueryDataParams qb){
		JResponse jr = publicDataService.preAuth(qb);
		if(jr!=null&&"0".equals(jr.getRetCode())){
			String dtId = qb.getDataID();
			String depID = qb.getDepID();
			JSONObject params = qb.parseJQueryParams();
			JSONObject result = null;
			try{
				result  = publicDataService.fetchData(dtId,depID,params);
				if(result!=null&&result.containsKey("done")&&result.getBooleanValue("done")){
					JSONObject data = result.getJSONObject("data");
					String encryptData = publicDataService.encryptAES(depID,data);
					jr = new JResponse("0","",encryptData);
				}else{
					String msg = result==null?"":result.getString("info");
					jr = new JResponse("9",msg,null);
				}
			}catch(Exception e){
				log.error("查询数据发生异常。dtID："+dtId+"，depID："+depID+"。异常信息:"+e.toString());
				jr = new JResponse("9","查询数据时发生异常，未能查找到数据。",null);
				return jr;
			}
		}
		return jr;
	}
	@RequestMapping(value = "/public/applyForExtractData",method = RequestMethod.POST)
	@ResponseBody
	public JResponse applyForExtractData(@RequestBody QueryDataParams qb){
		JResponse jr = publicDataService.preAuth(qb);
		if(jr!=null&&"0".equals(jr.getRetCode())){
			String dtId = qb.getDataID();
			String depID = qb.getDepID();
			JSONObject params = qb.parseJQueryParams();
			JSONObject result = null;
			try{
				result  = publicDataService.applyForExtractData(dtId,depID,params);
				if(result!=null&&result.containsKey("done")&&result.getBooleanValue("done")){
					JSONObject newid = new JSONObject();
					newid.put("applyID", result.get("applyID").toString());
					jr = new JResponse("0","",newid);
				}else{
					String msg = result==null?"":result.getString("info");
					jr = new JResponse("9",msg,null);
				}
			}catch(Exception e){
				log.error("申请抽取数据时发生异常。dtID："+dtId+"，depID："+depID+"。异常信息:"+e.toString());
				jr = new JResponse("9","申请抽取数时发生异常，未能提交申请。",null);
				return jr;
			}
		}
		return jr;
	}
	
	@RequestMapping(value = "/public/queryExtractInfo",method = RequestMethod.POST)
	@ResponseBody
	public JResponse queryExtractInfo(@RequestBody QueryDataParams qb){
		JResponse jr = publicDataService.preAuth(qb);
		if(jr!=null&&"0".equals(jr.getRetCode())){
			JSONObject params = qb.parseJQueryParams();
			String depID = qb.getDepID();
			String applyID = params.get("applyID").toString();
			JSONObject result = null;
			try{
				result  = publicDataService.queryExtractInfo(params);
				if(result!=null&&result.containsKey("total")){
					jr = new JResponse("0","",result);
				}else{
					String msg = result==null?"":result.getString("info");
					jr = new JResponse("9",msg,null);
				}
			}catch(Exception e){
				log.error("查询取数申请信息时发生异常。depID："+depID+"，applyID："+applyID+"。异常信息:"+e.toString());
				jr = new JResponse("9","查询取数申请信息时发生异常。",null);
				return jr;
			}
		}
		return jr;
	}
	@RequestMapping(value = "/public/downloadDataFile",method = RequestMethod.POST)
	@ResponseBody
	public void downloadDataFile(@RequestParam("expParams") String expParams){
		if(StringUtils.isEmpty(expParams)){
			//return new JResponse("9","缺少下载文件所需要的参数！",null);
			return ;
		}
		JSONObject params =JSONObject.parseObject(expParams);
		QueryDataParams qb = new QueryDataParams();
		qb.setDataID(params.getString("dataID"));
		qb.setDepID(params.getString("depID"));
		qb.setTimestamp(params.getString("timestamp"));
		qb.setSignature(params.getString("signature"));
		qb.setQueryParams(params.getString("queryParams"));
		JResponse jr = publicDataService.preAuth(qb);
		
		if(jr!=null&&"0".equals(jr.getRetCode())){
			JSONObject qparams = qb.parseJQueryParams();
			String depID = qb.getDepID();
			String resultID = qparams.get("resultID").toString();
        	HttpServletResponse response =null;
    		RequestAttributes requestAttributes = RequestContextHolder.currentRequestAttributes();
    		response = ((ServletRequestAttributes) requestAttributes).getResponse();
    		// response.setContentType("application/binary;charset=UTF-8");
			try{
				publicDataService.exportListData(response,resultID);
			}catch(Exception e){
				log.error("下载取数文件时发生异常。depID："+depID+"，resultID："+resultID+"。异常信息:"+e.toString());
				//jr = new JResponse("9","下载取数文件时发生异常。"+e.getMessage(),null);
				return ;
			}
		}
		jr.setRetCode("0");
		jr.setRetMsg("");
		return ;
	}
	
	@RequestMapping(value = "/updateDepSecret")
	@ResponseBody
	public JResponse updateDepSecret(@RequestParam String depID){
		JSONObject result = publicDataService.updateDepSecret(depID);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg(result.getString("info"));
		jr.setRetData(null);
		return jr;
	}
	@RequestMapping(value = "/updateDepAESkey")
	@ResponseBody
	public JResponse updateDepAESkey(@RequestParam String depID){
		JSONObject result = publicDataService.updateDepAESkey(depID);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg(result.getString("info"));
		jr.setRetData(null);
		return jr;
	}
	@RequestMapping(value = "/approveExtractApply")
	@ResponseBody
	public JResponse approveExtractApply(@RequestParam String applyID){
		JSONObject result = publicDataService.approveExtractApply(applyID);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg(result.getString("info"));
		jr.setRetData(null);
		return jr;
	}
	
	
	//通用接口
	@RequestMapping(value="/refreshDataSrcs")
	@ResponseBody
	public JResponse refreshDataSrcs(){
		JResponse jr = new JResponse();
		templatesLoader.refreshDataSrcs();
		jr.setRetCode("0");
		jr.setRetMsg("");
		return jr;
	}
	@RequestMapping(value="/refreshJSONOutputs")
	@ResponseBody
	public JResponse refreshJSONOutputs(){
		JResponse jr = new JResponse();
		templatesLoader.refreshJSONOutputs();
		jr.setRetCode("0");
		jr.setRetMsg("");
		return jr;
	}
	@RequestMapping(value = "/refreshConfig")
	@ResponseBody
    public JResponse refreshConfig() throws Exception {
        contextRefresher.refresh();
        JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg("配置已刷新！");
		return jr;
    }
	@RequestMapping(value = "/encryptStr")
    public String encryptStr(@RequestParam String str,@RequestParam(required=false) String password){
        String encryptStr = jasyptUtils.encypt(str,password);
        return encryptStr;
    }
	@RequestMapping(value = "/decryptStr")
    public String decryptStr(@RequestParam String str,@RequestParam(required=false) String password){
        String decryptStr = jasyptUtils.decypt(str,password);
        return decryptStr;
    }
	
	
	
	//测试接口
	@RequestMapping(value = "/sign")
	@ResponseBody
	public JResponse sign(@RequestParam String depID,@RequestParam String secret){
		long timestamp = System.currentTimeMillis();
		String plaintext = depID+secret+timestamp;
		String signatrue = BCrypt.hashpw(plaintext, BCrypt.gensalt());
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		JSONObject dt = new JSONObject();
		dt.put("signatrue", signatrue);
		dt.put("timestamp", timestamp);
		jr.setRetData(dt);
		return jr;
	}
	
	@RequestMapping(value = "/auth")
	@ResponseBody
	public JResponse auth(@RequestParam String depID,@RequestParam String timestamp,@RequestParam String signature){
		boolean valid = publicDataService.authenticate(depID,timestamp,signature);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg("签名验证结果："+valid);
		return jr;
	}
	
	@RequestMapping(value = "/decryptData",method = RequestMethod.POST)
	@ResponseBody
	public JResponse decryptData(@RequestParam String depID,@RequestParam String sData){
		String decryptData = publicDataService.decryptAES(depID,sData);
		JSONObject obj = JSONObject.parseObject(decryptData);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetData(obj);
		return jr;
	}
	
	@RequestMapping(value = "/decryptFileAES",method = RequestMethod.POST)
	@ResponseBody
	public JResponse decryptFileAES(@RequestParam String depID,@RequestParam String filename){
		String root = environment.getProperty("exportRoot","d:/exportRoot/");
		filename = (root.endsWith("/")? root : root + "/")+filename+".zip";
		JSONObject result = publicDataService.decryptFileAES(depID,filename);
		JResponse jr = new JResponse();
		jr.setRetCode("0");
		jr.setRetMsg(result.getString("info"));
		jr.setRetData(null);
		return jr;
	}
}

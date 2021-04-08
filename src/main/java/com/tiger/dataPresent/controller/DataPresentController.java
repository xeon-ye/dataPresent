package com.tiger.dataPresent.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.tiger.dataPresent.service.DataPresentService;
import com.tiger.dataPresent.utils.TemplatesLoader;
import com.tiger.dataPresent.utils.bean.RptDataJson;
import com.tiger.utils.JResponse;
import com.tiger.utils.JasyptUtils;

@CrossOrigin
@RestController
public class DataPresentController {
	private static Logger log = LoggerFactory.getLogger(DataPresentController.class);
	@Autowired
	private DataPresentService dataPresentService;
	@Autowired
	private TemplatesLoader templatesLoader;
	@Autowired
	private JasyptUtils jasyptUtils;
	
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

	@RequestMapping(value = "/encryptStr",method = RequestMethod.GET)
    public String encrypt(@RequestParam String str,@RequestParam(required=false) String password){
        String encryptStr = jasyptUtils.encypt(str,password);
        return encryptStr;
    }
}

package com.tiger.dataPresent.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;

import com.tiger.dataPresent.utils.bean.template.*;

@Service("templatesLoader")  
public class TemplatesLoader {
	private TemplatesLoader tLoader;
	private Map dataSrcMap;
	private List dataSrcTemplates;
	private Map JSONOutputMap;
	private List JSONOutputTemplates;
	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
    private Environment environment;
	
	public void loadDataSrcs(){
		String loadDs=environment.getProperty("loadDataSrcTemplates", "1");
		//数据源模板不是每次都需要加载，可以不加载。
		if("0".equals(loadDs)){
			return;
		}
		dataSrcMap=new HashMap();
		dataSrcTemplates = new ArrayList();
		loadTemplatesFromFile("dataSrc",dataSrcTemplates,dataSrcMap);
	}
	public void refreshDataSrcs(){
		dataSrcMap=new HashMap();
		dataSrcTemplates = new ArrayList();
		loadTemplatesFromFile("dataSrc",dataSrcTemplates,dataSrcMap);
	}
	public void loadJSONOutputs(){
		String loadOp=environment.getProperty("loadJSONOutputTemplates", "1");
		//响应输出模板不是每次都需要加载，可以不加载。
		if("0".equals(loadOp)){
			return;
		}
		JSONOutputMap=new HashMap();
		JSONOutputTemplates = new ArrayList();
		loadTemplatesFromFile("JSONOutput",JSONOutputTemplates,JSONOutputMap);
	}
	public void refreshJSONOutputs(){
		JSONOutputMap=new HashMap();
		JSONOutputTemplates = new ArrayList();
		loadTemplatesFromFile("JSONOutput",JSONOutputTemplates,JSONOutputMap);
	}
	
	public void loadTemplatesFromFile(String tmpType,List tslst,Map tsmap){
		String path=environment.getProperty(tmpType+"TemplatesPath", "");
		//List pathes=new ArrayList();
		InputStream is=null;
		try{
			//java.io.File dir=ResourceUtils.getFile(path);
			Resource[] pathes = applicationContext.getResources(path);
			//getAllFilesPath(dir,pathes);
			//System.out.println("共找到"+pathes.size()+"个文件！");
			//各个设计文件循环解析、加载。
			if(pathes!=null&&pathes.length>0){
				for(int i=0;i<pathes.length;i++){
					//文件流转化成string作为参数传递给解析器
					Resource xmlPath=(Resource)pathes[i];
					File tmpFile=xmlPath.getFile();
					String fname = tmpFile.getName();
					is=new FileInputStream(tmpFile) ;
					long contentLength = tmpFile.length();
					byte[] ba = new byte[(int)contentLength];
					is.read(ba);
					String tInfo = new String(ba,"utf-8");
					is.close();
					//设计内容，解析
					if("dataSrc".equals(tmpType)){
						try{
							loadDataSrcTemplate(tInfo,tslst,tsmap,fname);
						}catch(Exception e){
							System.out.println();
						}
					}else if("JSONOutput".equals(tmpType)){
						try{
							loadJSONOutputTemplate(tInfo,tslst,tsmap,fname);
						}catch(Exception e){
							System.out.println();
						}
					}
				}
				System.out.println(tmpType+"共解析"+tslst.size()+"个模板！");
				//按所在文件进行排序
				if(tslst!=null){
					Comparator infileCmp = ComparableComparator.getInstance();
					infileCmp = ComparatorUtils.nullHighComparator(infileCmp); // 允许null
			        // 声明要排序的对象的属性，并指明所使用的排序规则，如果不指明，则用默认排序
			        ArrayList<Object> sortFields = new ArrayList<Object>();
			        sortFields.add(new BeanComparator("infile", infileCmp)); // 主排序（第一排序）
			        // 创建一个排序链
			        ComparatorChain multiSort = new ComparatorChain(sortFields);
			        // 开始真正的排序，按照先主，后副的规则
			        Collections.sort(tslst, multiSort);
				}
			}
		}catch(Exception e){
			if(is!=null){
				try{
					is.close();
				}catch(Exception ex){};
			}
			System.out.print("加载"+tmpType+"模板信息时发生错误："+e.toString());
		}finally{
			try{
				is.close();
			}catch(Exception e){
			}
		}
	}
	private void loadDataSrcTemplate(String tInfo,List tslst,Map tsmap,String fileName) {
		DataSourceTemplateParser parser=DataSourceTemplateParser.getParser();
		parser.parseTemplateToDtSrc(tInfo,tslst,tsmap, fileName);
		return;
	}
	
	private void loadJSONOutputTemplate(String tInfo,List tslst,Map tsmap,String fileName) {
		JOutputParser parser=JOutputParser.getParser();
		parser.parseTemplateToJOutput(tInfo,tslst,tsmap, fileName);
	}
	public JOutput getJOutput(String jpID){
		if(jpID==null||"".equals(jpID))
			return null;
		if(JSONOutputMap==null){
			JSONOutputMap=new HashMap();
			JSONOutputTemplates = new ArrayList();
			loadTemplatesFromFile("JSONOutput",JSONOutputTemplates,JSONOutputMap);
		}
		JOutput jp=(JOutput)JSONOutputMap.get(jpID);
		return jp;
	}
	public Map getJSONOutputMap() {
		if(JSONOutputMap==null){
			JSONOutputMap=new HashMap();
			JSONOutputTemplates = new ArrayList();
			loadTemplatesFromFile("JSONOutput",JSONOutputTemplates,JSONOutputMap);
		}
		return JSONOutputMap;
	}
	public List getJSONOutputTemplates() {
		if(JSONOutputTemplates==null){
			JSONOutputMap=new HashMap();
			JSONOutputTemplates = new ArrayList();
			loadTemplatesFromFile("JSONOutput",JSONOutputTemplates,JSONOutputMap);
		}
		return JSONOutputTemplates;
	}
	public DataSrc getDataSrc(String dtID){
		if(dtID==null||"".equals(dtID))
			return null;
		if(dataSrcMap==null){
			dataSrcMap=new HashMap();
			dataSrcTemplates = new ArrayList();
			loadTemplatesFromFile("dataSrc",dataSrcTemplates,dataSrcMap);
		}
		DataSrc dts=(DataSrc)dataSrcMap.get(dtID);
		return dts;
	}
	public Map getDataSrcMap() {
		if(dataSrcMap==null){
			dataSrcMap=new HashMap();
			dataSrcTemplates = new ArrayList();
			loadTemplatesFromFile("dataSrc",dataSrcTemplates,dataSrcMap);
		}
		return dataSrcMap;
	}
	public List getDataSrcTemplates() {
		if(dataSrcTemplates==null){
			dataSrcMap=new HashMap();
			dataSrcTemplates = new ArrayList();
			loadTemplatesFromFile("dataSrc",dataSrcTemplates,dataSrcMap);
		}
		return dataSrcTemplates;
	}

	//获取指定目录下的设计文件，递归
	@SuppressWarnings("unchecked")
	private void getAllFilesPath(File dir,List paths)throws Exception{
		File[] fs = dir.listFiles(); 
		if(fs==null||fs.length==0)return;
		for(int i=0; i<fs.length; i++){ 
			if(fs[i].isDirectory()){
				System.out.println(fs[i].getAbsolutePath());
				//递归获取任务文件
				getAllFilesPath(fs[i],paths); 
			}else{
				paths.add(fs[i].getAbsolutePath());
				System.out.println(fs[i].getAbsolutePath());
			}
		} 
	}
}

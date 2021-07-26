package com.tiger.dataPresent.utils.job;

import java.io.*;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriter {
	private static Logger log = LoggerFactory.getLogger(FileWriter.class);
	//存储目录
	private String dir;
	private String rootDir;
	private String subDir;
	private String fileNameRoot;
	//记录总数
	private int recordCount;
	//文件数，分批保存为文件时使用
	private int fileCount = 0;
	//保存编码
	private String encode = "GBK";
	//每个文件的记录数
	private int rowsPerFile = 10000;
	//实际的文件中行数，计数器。
	private int rowsInFile = 0;
	private int outputCount = 0;
	//输出列的配置
	private String[] saveColumns;
	
	private File dataFile;
	private FileOutputStream dataOutputStream;
	private Workbook workbook;
	private String colSeparator="|";
	private String AESkey;
	private int encrypt=1;
	
	/**
	 * 按文件输出记录行
	* @param dataList
	* @return
	* @throws Exception
	 */
	public boolean writeTxt(String dtID,List dataList) throws IOException {
		try {
			if (dataFile == null) {
				this.dir=createSubDir(rootDir,subDir);
				dataFile = new File(this.dir + fileNameRoot +"_"+ (++fileCount) + ".txt");
				dataFile.createNewFile();
				dataOutputStream = new FileOutputStream(dataFile);
				rowsInFile = 0;
			}
			for (int i = 0; i<dataList.size(); i++) {
				List row = (List) dataList.get(i);
				if(row==null||row.size()==0)
					continue;
				String content = StringUtils.join(row, colSeparator);
				dataOutputStream.write(content.getBytes(encode));
				dataOutputStream.write("\r\n".getBytes(encode));
				rowsInFile++;
				outputCount++;
				if(rowsPerFile>0&&rowsInFile >= rowsPerFile){//rowsPerFile设置为非负数，才处理分文件。
					dataOutputStream.close();
					if(outputCount<recordCount){
						dataFile = new File(this.dir + fileNameRoot +"_"+ (++fileCount)+ ".txt");
						dataFile.createNewFile();
						dataOutputStream = new FileOutputStream(dataFile);
						rowsInFile = 0;
					}
				}
				if(outputCount>=recordCount){
					dataOutputStream.close();
				}
			}
		} catch (IOException ex) {
			dataOutputStream.close();
			log.error("将数据写入TXT文件时发生错误：{} ", ex);
			return false;
		}
		return true;
	}
	
	public void writeExcel(String dtID, List dataList, String format)throws IOException {
		try {
			if (workbook == null) {
				workbook = initExcel(format);
				rowsInFile = 0;
			}
			Sheet sheet = workbook.getSheetAt(0);
			for(int i=0;i<dataList.size();i++){
	    		List row = (List) dataList.get(i);
				if(row==null||row.size()==0)
					continue;
				Row sheetRow = sheet.createRow(rowsInFile+1);
				sheetRow.setHeight((short)500);
				for (int j = 0; j < saveColumns.length;  j++) {
					String strV = row.get(j).toString();
					Cell cell = sheetRow.createCell((short) j);
					cell.setCellType(Cell.CELL_TYPE_STRING);
					cell.setCellValue(strV==null?"":strV);
				}
				rowsInFile++;
				outputCount++;
				if(rowsPerFile>0&&rowsInFile >= rowsPerFile){
					this.dir=createSubDir(rootDir,subDir);
					String filePath = this.dir + fileNameRoot +"_"+ (++fileCount) ;
					if("1".equals(format)){
						filePath += ".xlsx";
			    	}else{
			    		filePath += ".xls";
			    	}
					dataOutputStream = new FileOutputStream(filePath);
					workbook.write(dataOutputStream);
					dataOutputStream.close();
					if(outputCount<recordCount){
						workbook = initExcel(format);
						rowsInFile = 0;
					}
				}
				if(outputCount>=recordCount){
					this.dir=createSubDir(rootDir,subDir);
					String filePath = this.dir + fileNameRoot +"_"+ (++fileCount) ;
					if("1".equals(format)){
						filePath += ".xlsx";
			    	}else{
			    		filePath += ".xls";
			    	}
					dataOutputStream = new FileOutputStream(filePath);
					workbook.write(dataOutputStream);
					dataOutputStream.close();
				}
			}
		} catch (IOException ex) {
			dataOutputStream.close();
			log.error("将数据写入Excel文件时发生错误：{} ", ex);
		}
	}
	private Workbook initExcel(String format){
		Workbook workbook = null;
    	if("1".equals(format)){
    		workbook = new XSSFWorkbook();
		}else{
			workbook = new HSSFWorkbook();
		}
    	Sheet sheet = workbook.createSheet();
    	sheet.setDefaultColumnWidth(16);
    	//表头信息
    	Row xlsrow = sheet.createRow(0);
    	xlsrow.setHeight((short) 500);
    	for (int i = 0; i < saveColumns.length;  i++) {
    		Cell cell = xlsrow.createCell(i,Cell.CELL_TYPE_STRING);
			cell.setCellValue(saveColumns[i]);
		}
    	return workbook;
	}
	
	public synchronized String createSubDir(String rootDir,String subDir) {
		String tmpSbDir = StringUtils.isEmpty(subDir)?"":subDir;
		String wholeDir = (rootDir.endsWith("/")? rootDir : rootDir + "/")+tmpSbDir;
		java.io.File dir=new java.io.File(wholeDir);
	    if(!dir.exists()){
	    	dir.mkdirs();
	    }else{
	    	dir.delete();
	    	dir.mkdirs();
	    }
		return wholeDir.endsWith("/")? wholeDir : wholeDir + "/";
	}

	/**
	 * 记录输出结束，输出一个文件，记录总体情况。
	 * 
	 * @throws Exception
	 */
	public void finishWriteFile() throws Exception {
		try {
			// 开始写info.txt
			File infoFile = new File(this.dir + fileNameRoot +"_"+"Info.txt");
			infoFile.createNewFile();
			FileOutputStream infoStream = new FileOutputStream(infoFile);
			StringBuffer info = new StringBuffer();
			info.append("[Infos]").append("\r\n");
			info.append("RecordCount=").append(this.recordCount).append("\r\n");
			info.append("RecordPerFile=").append(this.rowsPerFile).append("\r\n");
			info.append("DataFileCount=").append(this.fileCount ).append("\r\n");
			info.append("ColumnSeparator=").append(this.colSeparator).append("\r\n");
			info.append("FiledsName=");
			for (int i = 0; i < saveColumns.length;  i++) {
				String cn = saveColumns[i];
				if (i > 0) {
					info.append(colSeparator);
				}
				info.append(cn);
			}
			info.append("\r\n");
			infoStream.write(info.toString().getBytes(encode));
			infoStream.close();
		} catch (IOException ex) {
			throw new Exception("输出文件元数据信息时发生错误：{}", ex);
		}
	}
	public String getDir() {
		return dir;
	}

	public String getEncode() {
		return encode;
	}
	public void setEncode(String encode) {
		this.encode = encode;
	}
	public int getRowsPerFile() {
		return rowsPerFile;
	}
	public void setRowsPerFile(int rowsPerFile) {
		this.rowsPerFile = rowsPerFile;
	}
	public String[] getSaveColumns() {
		return saveColumns;
	}
	public void setSaveColumns(String[] saveColumns) {
		this.saveColumns = saveColumns;
	}
	public String getColSeparator() {
		return colSeparator;
	}

	public void setColSeparator(String colSeparator) {
		this.colSeparator = colSeparator;
	}
	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
	public String getRootDir() {
		return rootDir;
	}
	public void setRootDir(String rootDir) {
		this.rootDir = rootDir;
	}
	public String getSubDir() {
		return subDir;
	}
	public void setSubDir(String subDir) {
		this.subDir = subDir;
	}
	public String getFileNameRoot() {
		return fileNameRoot;
	}
	public void setFileNameRoot(String fileNameRoot) {
		this.fileNameRoot = fileNameRoot;
	}
	public String getAESkey() {
		return AESkey;
	}

	public void setAESkey(String aESkey) {
		AESkey = aESkey;
	}

	public int getEncrypt() {
		return encrypt;
	}

	public void setEncrypt(int encrypt) {
		this.encrypt = encrypt;
	}
}

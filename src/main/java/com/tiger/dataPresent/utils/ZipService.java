package com.tiger.dataPresent.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service("zipService")
public class ZipService {
	private static Logger log = LoggerFactory.getLogger(ZipService.class);

	/**
	 * zip压缩文件
	 * 
	 * @param dir 待压缩的文件/文件夹
	 * @param zippath 压缩后的文件名
	 */
	public void zip(String dir, String zippath) {
		List<String> paths = getFiles(dir);
		compressFilesZip(paths.toArray(new String[paths.size()]), zippath, dir);
	}

	/**
	 * 递归取到当前目录所有文件
	 * 
	 * @param dir
	 * @return
	 */
	public List<String> getFiles(String dir) {
		List<String> lstFiles = null;
		if (lstFiles == null) {
			lstFiles = new ArrayList<String>();
		}
		File file = new File(dir);
		if (!file.isDirectory()) {
			lstFiles.add(file.getAbsolutePath());
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				if (f.isDirectory()) {
					lstFiles.add(f.getAbsolutePath());
					lstFiles.addAll(getFiles(f.getAbsolutePath()));
				} else {
					String str = f.getAbsolutePath();
					lstFiles.add(str);
				}
			}
		}
		return lstFiles;
	}

	 public void delFiles(String dir){  
        File file = new File(dir);
        if(!file.isDirectory()){
        	file.delete();
        }else{
	        File [] files = file.listFiles();  
	        for(File f : files){  
                String str =f.getAbsolutePath();  
                delFiles(str);  
	        }
	        file.delete();
        }
    }  
	 
	public void compressFilesZip(String[] files, String zipFilePath, String dir) {
		if (files == null || files.length <= 0) {
			log.info("指定要压缩的文件目录不存在或无文件。");
			return;
		}
		ZipArchiveOutputStream zaos = null;
		try {
			File zipFile = new File(zipFilePath);
			zaos = new ZipArchiveOutputStream(zipFile);
			zaos.setUseZip64(Zip64Mode.AsNeeded);
			// 将每个文件用ZipArchiveEntry封装
			// 再用ZipArchiveOutputStream写到压缩文件中
			for (String strfile : files) {
				InputStream is = null;
				try {
					File file = new File(strfile);
					String name = getFilePathName(dir, strfile);
					ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(file, name);
					zaos.putArchiveEntry(zipArchiveEntry);
					if (file.isDirectory()) {
						continue;
					}
					is = new BufferedInputStream(new FileInputStream(file));
					byte[] buffer = new byte[1024];
					int len = -1;
					while ((len = is.read(buffer)) != -1) {
						// 把缓冲区的字节写入到ZipArchiveEntry
						zaos.write(buffer, 0, len);
					}
				} catch (Exception e) {
					log.error("压缩过程发生IO错误：" + e.toString());
					return;
				} finally {
					if (is != null) {
						is.close();
					}
				}
			}
			zaos.closeArchiveEntry();
			zaos.finish();
		} catch (Exception e) {
			log.error("压缩过程发生错误：" + e.toString());
			return;
		} finally {
			IOUtils.closeQuietly(zaos);
		}
	}

	public String getFilePathName(String dir, String path) {
		File file = new File(dir);
		String rootDir = file.getAbsolutePath();
		String p = path.replace(rootDir + File.separator, "");
		p = p.replace("\\", "/");
		return p;
	}

	public void decompressZip(String zipFilePath, String saveFileDir) {
		if (isEndsWithZip(zipFilePath)) {
			File file = new File(zipFilePath);
			if (file.exists()) {
				InputStream is = null;
				// can read Zip archives
				ZipArchiveInputStream zais = null;
				try {
					saveFileDir = saveFileDir.endsWith(File.separator) ? saveFileDir : saveFileDir + File.separator;
					File saveRoot = new File(saveFileDir);
					// 文件对象创建后，若有重复的，先将其删除，全新创建。
					if (saveRoot.exists()) {
						delFiles(saveFileDir);
					}
					saveRoot.mkdirs();
					is = new FileInputStream(file);
					zais = new ZipArchiveInputStream(is);
					ArchiveEntry archiveEntry = null;
					while ((archiveEntry = zais.getNextEntry()) != null) {
						// 获取文件名
						String entryFileName = archiveEntry.getName();
						// 构造解压出来的文件存放路径
						String entryFilePath = saveFileDir + entryFileName;
						OutputStream os = null;
						try {
							// 把解压出来的文件写到指定路径
							File entryFile = new File(entryFilePath);
							os = new FileOutputStream(entryFile);
							byte[] buffer = new byte[1024 * 5];

							int length = -1;
							while ((length = zais.read(buffer)) != -1) {
								os.write(buffer, 0, length);
							}
							os.flush();
						} catch (IOException e) {
							throw new IOException(e);
						} finally {
							IOUtils.closeQuietly(os);
						}

					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					try {
						if (zais != null) {
							zais.close();
						}
						if (is != null) {
							is.close();
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
	}

	/**
	 * 判断文件名是否以.zip为后缀
	 * 
	 * @param fileName
	 *            需要判断的文件名
	 * @return 是zip文件返回true,否则返回false
	 */
	public boolean isEndsWithZip(String fileName) {
		boolean flag = false;
		if (fileName != null && !"".equals(fileName.trim())) {
			if (fileName.endsWith(".ZIP") || fileName.endsWith(".zip")) {
				flag = true;
			}
		}
		return flag;
	}
}

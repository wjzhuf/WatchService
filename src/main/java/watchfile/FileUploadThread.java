package watchfile;

import java.io.File;
import org.apache.log4j.Logger;
import watchfile.common.Conmmon;
import watchfile.common.FileOperaLog;
import watchfile.common.Util;

/**
 * 实现hdfs文件上传
 * 
 * @author Administrator
 *
 */
public class FileUploadThread implements Runnable {
	private static Logger logger = Logger.getLogger("Log");
	private String fileName;
	private String suffName;
	private String bizName;
	private String xmlName;
	private int count;

	public FileUploadThread(String fileName, String bizName, String suffName, String xmlName, int count) {
		this.fileName = fileName;
		this.bizName = bizName;
		this.suffName = suffName;
		this.xmlName = xmlName;
		this.count = count;
	}

	@Override
	public void run() {
		logger.info(Thread.currentThread().getName() + " upload files  [ " + fileName + "]");
		processCommand(fileName, bizName, suffName, xmlName);
		System.out.println(Thread.currentThread().getName() + " Complete file upload  [ " + fileName + "]");
	}

	/**
	 * 文件上传
	 * 
	 * @param fileName
	 */
	private void processCommand(String fileName, String bizName, String suffName, String xmlName) {
		String datName = fileName.substring(fileName.lastIndexOf(File.separator) + 1, fileName.length());
		String datpath = fileName.substring(0, fileName.lastIndexOf(File.separator));
		logger.info("dat文件名：" + datName );
		logger.info("dat文件路径：    " + datpath);

		String shellString = "/bin/sh hdfs.sh  " + datpath + "  " + WatchFile.HDSSPATH + bizName
				+ " " + Util.APP_NAME + " " + suffName + "  " + datName;
		try {
			Process process = Runtime.getRuntime().exec(shellString);
			int exitValue = process.waitFor();

			if (0 != exitValue) {
				System.out.println(exitValue);
				// 持久化控制表 0-等待上传 1-上传成功 2上传失败
				logger.info("PUT文件：    " + fileName +   " 到HDFS失败");
				Util.controlUpdateDao(xmlName, fileName, "2");
			} else {
				// 持久化控制表 0-等待上传 1-上传成功 2上传失败

				if (suffName.equalsIgnoreCase("xml")) {
					int au = Util.controlSelectDao(xmlName);

					if (au == count) {
						FileOperaLog flo = new FileOperaLog();
						flo.setCarryState("FINISH");
						flo.setChefileName(xmlName);
						flo.setCuttState(Conmmon.VERIFILEBYTAR_PUTOHDFS_SUCC);
						flo.setStateremark("VERIFILEBYTAR_PUTOHDFS_SUCC");
						logger.info("文件PUT到HDFS目录成功");
						Util.updateMessageDao(flo);
					} else {
						FileOperaLog flo = new FileOperaLog();
						flo.setCarryState("FAIL");
						flo.setChefileName(xmlName);
						flo.setCuttState(Conmmon.VERIFILEBYTAR_PUTOHDFS_FAIL);
						flo.setStateremark("VERIFILEBYTAR_PUTOHDFS_FAIL");
						logger.info("文件PUT到HDFS目录失败");
						Util.updateMessageDao(flo);
					}

				}
				logger.info("PUT文件：    " + fileName +   " 到HDFS成功");
				Util.controlUpdateDao(xmlName, fileName, "1");
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.info("PUT文件：    " + fileName +   " 到HDFS失败");
			// 持久化控制表 0-等待上传 1-上传成功 2上传失败
			Util.controlUpdateDao(xmlName, fileName, "2");
		}
		logger.info("shellString：    " + shellString);
	}

	@Override
	public String toString() {
		return this.fileName;
	}
}
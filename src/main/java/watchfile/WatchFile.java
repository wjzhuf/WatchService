package watchfile;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import watchfile.common.Conmmon;
import watchfile.common.FileOperaLog;
import watchfile.common.Util;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 实现文件和目录变更的监控(包含各级子目录)
 * 
 */

public class WatchFile {
	private static Logger logger = Logger.getLogger("Log");
	private final WatchService watcher;
	private final Map<WatchKey, Path> keys;
	private boolean trace = false;
	SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss.SSS");
	private static int THREADPUT_NUM = 5;  //上传hdfs线程池默认大小
	private static int THREADUNTAR_NUM = 5; //解压tar包线程池默认大小
	private static int CREFILE_CYCLE = 2; //默认监控周期
	private static String RECEIVE_PATH;
	private static String TMP_PATH;
	public static String HDSSPATH;
	ExecutorService executorput = Executors.newCachedThreadPool();

	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}
	
	/**
	 * log4j读取路径
	 * 
	 * @param dir
	 * @throws IOException
	 */
	static {   
        PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator   
                + "log4j.properties");   
    } 
	/**
	 * 将dir目录注册到WatchService中
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void register(Path dir) throws IOException {
		WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
		if (trace) {
			Path prev = keys.get(key);
			if (prev == null) {
				logger.info("注册监听新增目录    " + dir);
			} else {
				if (!dir.equals(prev)) {
					logger.info(prev + "更新为    " + dir);
				}
			}
		}
		keys.put(key, dir);
	}

	/**
	 * 注册指定目录及其子目录到WatchService中
	 * 
	 * @param start
	 * @throws IOException
	 */
	private void registerAll(final Path start) throws IOException {
		Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				register(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * 创建WatchService并注册监听根目录
	 * 
	 * @param dir
	 * @param recursive
	 * @throws IOException
	 */
	WatchFile(Path dir, boolean recursive) throws IOException {
		this.watcher = FileSystems.getDefault().newWatchService();
		this.keys = new HashMap<WatchKey, Path>();

		if (recursive) {
			logger.info("开始监听目录    " + dir);
			registerAll(dir);
			logger.info("监听建立成功");
			// 获取PID信息
			RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
			String name = runtime.getName(); // format: "pid@hostname"
			Util.controlPidbyAppDao(Util.APP_NAME, name.substring(0, name.indexOf('@')));
			logger.info("获取PID信息入库    " + name.substring(0, name.indexOf('@')));

			// System.out.println(name.substring(0, name.indexOf('@')));
		} else {
			register(dir);
		}

		this.trace = true;
	}

	/**
	 * xml文件初始化装填记录
	 * 
	 * @param filePath
	 * @param xmlName
	 * @param cuttstate
	 * @param carrystate
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public boolean initXml(String filePath, String xmlName, String cuttstate, String carrystate)
			throws UnsupportedEncodingException {
		boolean mainFla = true;
		FileOperaLog xmlsta = new FileOperaLog();
		xmlsta.setFilePath(filePath);
		xmlsta.setChefileName(xmlName);
		xmlsta.setTarName("");
		xmlsta.setCuttState(cuttstate);// 00
		xmlsta.setCarryState(carrystate);
		xmlsta.setCreateTime(Util.sdf.format(new Date()));
		xmlsta.setUpdateTime(Util.sdf.format(new Date()));
		xmlsta.setStateremark("INIT STATE....");
		// 初始化验证文件状态
		if (!Util.messageDao(xmlsta)) {
			logger.info("初始化就绪文件数据失败");
			mainFla = false;
		}
		logger.info("初始化就绪文件状态：    " + cuttstate + "     执行状态： " + carrystate);
		return mainFla;
	}

	/**
	 * 验证校验文件名称
	 * 
	 * @param xmlName
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public boolean checkXmlFileName(String xmlName) throws UnsupportedEncodingException {
		boolean mainFla = true;
		String[] fileL = xmlName.split("_");
		if (fileL.length != 5) {
			FileOperaLog flo = new FileOperaLog();
			flo.setCarryState("FAIL");
			flo.setChefileName(xmlName);
			flo.setCuttState(Conmmon.VERIFILE_NAME_INVALID);
			flo.setStateremark("VERIFILE_NAME_INVALID");
			// 校验文件结构不合法
			if (Util.updateMessageDao(flo)) {
				logger.info("校验文件名称不正确：    " + Conmmon.VERIFILE_NAME_INVALID + "     执行状态： " + "FAIL");
				mainFla = false;
			}
			// String chefile_name,String cutt_state,String carry_state
		} else {
			String date = fileL[3];
			// 检查校验文件日期是否合法
			if (!Util.isValidDate(date)) {

				FileOperaLog flo = new FileOperaLog();
				flo.setCarryState("FAIL");
				flo.setChefileName(xmlName);
				flo.setCuttState(Conmmon.VERIFILE_DDATE_INNALID);
				flo.setStateremark("VERIFILE_DDATE_INNALID");
				// 校验文件日期不合法
				if (Util.updateMessageDao(flo)) {
					logger.info("就绪文件数据日期非法：    " + Conmmon.VERIFILE_NAME_INVALID + "     执行状态： " + "FAIL");
					mainFla = false;
				}

			}

		}
		logger.info("校验就绪文件成功");
		return mainFla;
	}

	/**
	 * 检查就绪文件是否可以打开
	 * 
	 * @param xmlPath
	 * @param xmlName
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private boolean checkFileSta(String xmlPath, String xmlName) throws UnsupportedEncodingException {
		boolean mainFla = true;
		if (!Util.openFile(xmlPath)) {
			FileOperaLog fol = new FileOperaLog();
			fol.setCarryState("FAIL");
			fol.setChefileName(xmlName);
			fol.setCuttState(Conmmon.VERIFILE_CANNOT_OPEN);
			fol.setStateremark("VERIFILE_CANNOT_OPEN");
			if (Util.updateMessageDao(fol)) {
				logger.info("就绪文件无法打开：    " + Conmmon.VERIFILE_CANNOT_OPEN + "     执行状态： " + "FAIL");
				mainFla = false;
			}
		}
		logger.info("就绪文件校验可以打开");
		return mainFla;
	}

	/**
	 * 检查文件是否存在
	 * 
	 * @param tarPath
	 * @param xmlName
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private boolean checkFileExist(String tarPath, String xmlName) throws UnsupportedEncodingException {
		boolean mainFla = true;
		File tarfi = new File(tarPath);
		// 判断压缩文件是否存在
		if (tarfi.exists() && tarfi.isFile()) {
			// tar判断是否损坏
			if (!Util.openFile(tarPath)) {
				FileOperaLog fol = new FileOperaLog();
				fol.setChefileName(xmlName);
				fol.setCarryState("FAIL");
				fol.setCuttState(Conmmon.DATFILE_CANNOT_OPEN);
				fol.setStateremark("DATFILE_CANNOT_OPEN");
				if (Util.updateMessageDao(fol)) {
					logger.info("接口数据文件无法打开：    " + Conmmon.DATFILE_CANNOT_OPEN + "     执行状态： " + "FAIL");
					mainFla = false;
				}
			}
		} else {
			FileOperaLog fol = new FileOperaLog();
			fol.setChefileName(xmlName);
			fol.setCarryState("FAIL");
			fol.setCuttState(Conmmon.DATFILE_NOT_EXIST);
			fol.setStateremark("DATFILE_NOT_EXIST");
			if (Util.updateMessageDao(fol)) {
				logger.info("接口数据文件不存在：    " + Conmmon.DATFILE_NOT_EXIST + "     执行状态： " + "FAIL");
				mainFla = false;
			}
		}
		logger.info("校验接口数据文件通过 ");
		return mainFla;
	}

	/**
	 * 校验接口文件名
	 * 
	 * @param fileName
	 * @param xmlName
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private boolean checkFileNameAndDate(String fileName, String xmlName) throws UnsupportedEncodingException {
		boolean che = true;
		if (fileName.isEmpty()) {
			logger.info("接口数据文件日期不合法");
			che = false;
		} else {
			String[] len = fileName.split("_");
			if (len.length != 6) {
				logger.info("接口数据文件日期不合法");
				che = false;
			}
			if (len.length == 6) {
				// 判断日期是否合法
				String dd = len[3];
				if (!Util.isValidDate(dd)) {
					logger.info("接口数据文件日期不合法");
					che = false;
				}
			}
		}
		if (!che) {
			FileOperaLog fol = new FileOperaLog();
			fol.setCarryState("FAIL");
			fol.setChefileName(xmlName);
			fol.setCuttState(Conmmon.DATFILE_NAME_INVALID);
			fol.setStateremark("DATFILE_NAME_INVALID");
			if (Util.updateMessageDao(fol)) {
				logger.info("接口文件名与规则不符：    " + Conmmon.DATFILE_NAME_INVALID + "     执行状态： " + "FAIL");
				che = false;
			}
		}
		logger.info("接口数据文件合法");
		return che;
	}

	/**
	 * 
	 * @param fileName
	 * @param xmlName
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private boolean checkFileSize(String fileSize, String xmlName) throws UnsupportedEncodingException {
		boolean che = true;
		if (!Util.isNumeric(fileSize)) {
			che = false;
			FileOperaLog fol = new FileOperaLog();
			fol.setCarryState("FAIL");
			fol.setChefileName(xmlName);
			fol.setCuttState(Conmmon.DATFILE_LENGTH_ERROR);
			fol.setStateremark("DATFILE_LENGTH_ERROR");
			logger.info("文件大小不符：    " + Conmmon.DATFILE_LENGTH_ERROR + "     执行状态： " + "FAIL");
			Util.updateMessageDao(fol);
		}
		logger.info("文件大小验证成功");
		return che;
	}

	/**
	 * 解压失败
	 * 
	 * @param xmlName
	 * @throws UnsupportedEncodingException
	 */
	private void untarStatus(String xmlName) throws UnsupportedEncodingException {
		FileOperaLog fol = new FileOperaLog();
		fol.setCarryState("FAIL");
		fol.setChefileName(xmlName);
		fol.setCuttState(Conmmon.TAR_UNZIP_FAIL);
		fol.setStateremark("TAR_UNZIP_FAIL");
		logger.info("TAR包解压失败：    " + Conmmon.TAR_UNZIP_FAIL + "     执行状态： " + "FAIL");
		Util.updateMessageDao(fol);
	}

	/**
	 * 文件复制
	 * 
	 * @param fromFilePath
	 * @param toPath
	 * @param preName
	 */
	private boolean moveFile(String fromFilePath, String toPath, String preName) {
		boolean fla = true;

		try {
			final Path copy_from = Paths.get(fromFilePath);
			final Path copy_to = Paths.get(toPath + preName);
			Files.copy(copy_from, copy_to, NOFOLLOW_LINKS);
			logger.info("MOVE文件到指定目录成功");
		} catch (Exception e) {
			logger.info("MOVE文件到指定目录失败");
			fla = false;
			e.printStackTrace();
		}

		return fla;
	}

	/**
	 * 处理文件和文件夹事件
	 */
	void processEvents() {

		for (;;) {

			WatchKey key;
			try {
				key = watcher.take();
			} catch (InterruptedException x) {
				return;
			}

			Path dir = keys.get(key);
			if (dir == null) {
				System.err.println("WatchKey not recognized!!");
				continue;
			}

			for (WatchEvent<?> event : key.pollEvents()) {
				final Kind<?> kind = event.kind();

				// TBD - provide example of how OVERFLOW event is handled
				if (kind == OVERFLOW) {
					continue;
				}

				WatchEvent<Path> ev = cast(event);
				Path name = ev.context();
				final Path child = dir.resolve(name);

				logger.info(event.kind().name() + " " + child);

				// 增加线程池处理业务逻辑
				boolean valid = key.reset();
				if (!valid) {
					keys.remove(key);
					// all directories are inaccessible
					if (keys.isEmpty()) {
						break;
					}
				}

				// 持久化控制表
				executorput.execute(new Runnable() {
					@Override
					public void run() {
						if (kind == ENTRY_CREATE) {
							try {
								if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
									registerAll(child);
								}

								// 判断监控周期
								String listenerDate = Util.sdfymd.format(new Date());
								String parentPath = child.getParent().toString();

								String bizName = parentPath.substring(parentPath.lastIndexOf(File.separator) + 1,
										parentPath.length());

								String middle = parentPath.substring(0, parentPath.lastIndexOf(File.separator));
								String pathDate = middle.substring(middle.lastIndexOf(File.separator) + 1,
										middle.length());

								// 如果超过监控周期则跳过
								if (!Util.isValidDate(pathDate)) {
									logger.info("超过监控周期");
									return;
								}
								if ((Integer.parseInt(listenerDate) - Integer.parseInt(pathDate)) < CREFILE_CYCLE) {

									// 文件绝对路径
									String xmlPath = child.toString();
									// 文件名称
									String xmlName = child.getFileName().toString();
									// 文件后缀
									String prefix = xmlName.substring(xmlName.lastIndexOf(".") + 1);
									// 总标记
									boolean mainFla = true;
									// 监控到xml文件创建,验证xml文件是否曾经被处理过
									FileOperaLog flo = new FileOperaLog();
									flo.setChefileName(xmlName);

									if (prefix.equalsIgnoreCase("xml") && (Util.isTarexit(flo).equalsIgnoreCase("2"))) {

										// 第一步入库暂存xml初始化状态,便于后续更新
										mainFla = initXml(xmlPath, xmlName, Conmmon.INIT_STATE, "RUNNING");

										// 初始化成功后读取并验证校验文件
										if (mainFla) {
											// 校验数据日期 ，xml文件名称包含五部分
											// 原系统_data_系统简称_YYYYMMDD_重传序号.xml
											mainFla = checkXmlFileName(xmlName);
										}

										// 校验文件是否可打
										if (mainFla) {
											// 校验文件是否可打
											mainFla = checkFileSta(xmlPath, xmlName);
										}
										String tarPath = xmlPath.substring(0, xmlPath.length() - 4) + ".tar";
										if (mainFla) {
											// 判断tar包是否存在
											mainFla = checkFileExist(tarPath, xmlName);
										}
										String endPath = "";
										String endTarPath = "";

										// TMP_PATH 移动文件到指定目录
										if (mainFla) {
											// move tar
											mainFla = moveFile(tarPath,
													TMP_PATH + File.separator + pathDate + File.separator + bizName
															+ File.separator,
													xmlName.substring(0, xmlName.lastIndexOf(".") + 1) + "tar");
										} else {
											flo.setCarryState("FAIL");
											flo.setCuttState(Conmmon.VERIFILEBYTAR_PUTTOTMP_FAIL);
											flo.setStateremark("VERIFILEBYTAR_PUTTOTMP_FAIL");
											logger.info("MOVE就绪文件和TAR包到临时目录失败：    "
													+ Conmmon.VERIFILEBYTAR_PUTTOTMP_FAIL + "     执行状态： " + "FAIL");
											Util.updateMessageDao(flo);
										}

										if (mainFla) {
											endTarPath = TMP_PATH + File.separator + pathDate + File.separator + bizName
													+ File.separator
													+ xmlName.substring(0, xmlName.lastIndexOf(".") + 1) + "tar";
											// move xml
											mainFla = moveFile(xmlPath, TMP_PATH + File.separator + pathDate
													+ File.separator + bizName + File.separator, xmlName);
										} else {
											flo.setCarryState("FAIL");
											flo.setCuttState(Conmmon.VERIFILEBYTAR_PUTTOTMP_FAIL);
											flo.setStateremark("VERIFILEBYTAR_PUTTOTMP_FAIL");
											logger.info("MOVE就绪文件和TAR包到临时目录失败：    "
													+ Conmmon.VERIFILEBYTAR_PUTTOTMP_FAIL + "     执行状态： " + "FAIL");
											Util.updateMessageDao(flo);
										}
										HashMap<String, String> xmlFile = new LinkedHashMap<String, String>();

										// 将init状态修改为已经移动到tmp文件夹中的状态

										flo.setCarryState("RUNNING");
										flo.setCuttState(Conmmon.VERIFILEBYTAR_PUTTOTMP_SUCC);
										flo.setStateremark("VERIFILEBYTAR_PUTTOTMP_SUCC");
										logger.info("MOVE就绪文件和TAR包到临时目录成功");
										Util.updateMessageDao(flo);

										// 解析xml就绪文件
										if (mainFla) {

											endPath = TMP_PATH + File.separator + pathDate + File.separator + bizName
													+ File.separator;

											// 校验xml中接口文件名规则
											FileInputStream fis = new FileInputStream(new File(xmlPath));
											Document document = Util.parse(fis);
											Element rootElement = document.getDocumentElement();
											NodeList allNode = rootElement.getElementsByTagName("file");
											int countSize = 0;
											for (int i = 0; i < allNode.getLength(); i++) {
												String fileName = document.getElementsByTagName("filename").item(i)
														.getTextContent();
												String fileSize = document.getElementsByTagName("filesize").item(i)
														.getTextContent();
												// 校验xml文件中接口文件名和日期是否非法
												mainFla = checkFileNameAndDate(fileName, xmlName);

												if (mainFla) {
													mainFla = checkFileSize(fileSize, xmlName);
													// 如果合法
													if (mainFla) {
														countSize = countSize + 1;
														xmlFile.put(fileName, fileSize);
													}
												}
											}

											if (countSize == allNode.getLength()) {
												mainFla = true;
											} else {
												mainFla = false;
											}

											// 关闭xml文件读取
											fis.close();
										}

										List<String> filesPath = new ArrayList<String>();

										/*** 从文件解压开始考虑后续流程 ***/
										// 解压缩tar文件
										if (mainFla) {
											// 解压缩tar包到当前文件夹 tmp目录

											HashMap ma = Untargz.run(endTarPath, endPath);

											mainFla = (boolean) ma.get("mainFla");
											List<String> tarZ = (ArrayList<String>) ma.get("untar");
											// 解压缩Z包到当前文件夹 tmp目录
											ExecutorService executor = Executors.newFixedThreadPool(THREADUNTAR_NUM);
											// upload pool
											for (String z : tarZ) {
												// 持久化控制表
												Runnable worker = new FileUnZThread(z, endPath);
												executor.execute(worker);
											}
											executor.shutdown();
											while (!executor.isTerminated()) {
											}

											logger.info("Finished all untar Z File");

											// 解压成功
											if (mainFla) {
												flo.setCarryState("RUNNING");
												flo.setChefileName(xmlName);
												flo.setCuttState(Conmmon.TAR_UNZIP_SUCC);
												flo.setStateremark("TAR_UNZIP_SUCC");
												logger.info("TAR包解压成功：    " + Conmmon.TAR_UNZIP_SUCC + "     执行状态： "
														+ "RUNNING");
												Util.updateMessageDao(flo);

												Iterator<?> it = xmlFile.entrySet().iterator();
												String mapFileName, mapFileSize;
												while (it.hasNext()) {
													Map.Entry entry = (Map.Entry) it.next();
													mapFileName = entry.getKey().toString();
													mapFileSize = entry.getValue().toString();
													
													File mapfi = new File(endPath + mapFileName);
													if (mapfi.exists() && mapfi.isFile()) {
														// 判断数据文件是否可以打开
														if (Util.openFile(xmlPath)) {
															// 判断文件大小是否相当
															if (!String.valueOf(mapfi.length())
																	.equalsIgnoreCase(mapFileSize)) {

																flo.setChefileName(xmlName);
																flo.setCarryState("FAIL");
																flo.setStateremark("DATFILE_LENGTH_ERROR");
																flo.setCuttState(Conmmon.DATFILE_LENGTH_ERROR);
																logger.info("文件大小不符：    " + Conmmon.DATFILE_LENGTH_ERROR
																		+ "     执行状态： " + "FAIL");
																Util.updateMessageDao(flo);
																mainFla = false;
															} else {
																// 存储待上传文件列表
																filesPath.add(endPath + mapFileName);
															}
														} else {
															flo.setChefileName(xmlName);
															flo.setCarryState("FAIL");
															flo.setCuttState(Conmmon.DATFILE_CANNOT_OPEN);
															flo.setStateremark("DATFILE_CANNOT_OPEN");
															logger.info("接口数据文件无法打开：    " + Conmmon.DATFILE_CANNOT_OPEN
																	+ "     执行状态： " + "FAIL");
															Util.updateMessageDao(flo);
															mainFla = false;
														}

													} else {
														// 接口数据文件不存在
														flo.setChefileName(xmlName);
														flo.setCarryState("FAIL");
														flo.setCuttState(Conmmon.DATFILE_NOT_EXIST);
														flo.setStateremark("DATFILE_NOT_EXIST");
														logger.info("接口数据文件不存在：    " + Conmmon.DATFILE_NOT_EXIST
																+ "     执行状态： " + "FAIL");
														Util.updateMessageDao(flo);
														mainFla = false;

													}
												}
											} else {
												// 解压失败
												logger.info("解压失败!!!");
												untarStatus(xmlName);
											}
										}

										// 总标记为true时候加入hdfs待上传队列
										if (mainFla) {
											ExecutorService executor = Executors.newFixedThreadPool(THREADPUT_NUM);
											// upload pool
											for (String fpath : filesPath) {
												// 持久化控制表 0-等待上传 1-上传成功 2上传失败
												Util.controlDao(xmlName, fpath, "0");
												Runnable worker = new FileUploadThread(fpath,
														pathDate + File.separator + bizName, "dat", xmlName, 0);
												executor.execute(worker);
											}
											executor.shutdown();
											while (!executor.isTerminated()) {
											}

											logger.info("Finished all threads");

											// 持久化控制表xml文件 0-等待上传 1-上传成功 2上传失败
											Util.controlDao(xmlName, xmlPath, "0");
											// chuan xml
											Thread aa = new Thread(
													new FileUploadThread(xmlPath, pathDate + File.separator + bizName,
															"xml", xmlName, filesPath.size()));
											aa.setName("upload XML");
											aa.start();

										}
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
								logger.info("error!");
							}
						}
					}
				});
			}
		}
	}

	public static void release(Connection conn, Statement st) {
		if (st != null) {
			try {
				st.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {

				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 程序入口
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			boolean recursive = true;
//			 InputStream in =
//			 WatchFile.class.getClassLoader().getResourceAsStream("app.properties");
//			 Properties prop = new Properties();
//			 prop.load(in);

			Properties prop = new Properties();
			File file = new File("app.properties");
			FileInputStream fis = new FileInputStream(file);
			prop.load(fis);
			fis.close();

			if (prop.getProperty("appName") == null) {
				logger.info("appName property not exits in app.properties");
				System.exit(-1);
			}
			Util.APP_NAME = prop.getProperty("appName");
			// 接收文件目录
			if (prop.getProperty("receivePath") == null) {
				logger.info("receivePath property not exits in app.properties");
				System.exit(-1);
			}
			RECEIVE_PATH = prop.getProperty("receivePath");
			if (RECEIVE_PATH.substring(RECEIVE_PATH.length() - 1, RECEIVE_PATH.length())
					.equalsIgnoreCase(File.separator)) {
				RECEIVE_PATH = RECEIVE_PATH.substring(0, RECEIVE_PATH.length() - 1);
			}

			// 临时文件目录
			if (prop.getProperty("tmpPath") == null) {
				logger.info("tmpPath property not exits in app.properties");
				System.exit(-1);
			}
			TMP_PATH = prop.getProperty("tmpPath");
			if (TMP_PATH.substring(TMP_PATH.length() - 1, TMP_PATH.length()).equalsIgnoreCase(File.separator)) {
				TMP_PATH = TMP_PATH.substring(0, TMP_PATH.length() - 1);
			}
			// 文件监控周期
			if (prop.getProperty("creFileCycle") != null
					&& Integer.parseInt(prop.getProperty("creFileCycle").trim()) <= 0) {
				logger.info("creFileCycle illegal");
				System.exit(-1);
			}
			CREFILE_CYCLE = Integer.parseInt(prop.getProperty("creFileCycle"));

			// 临时文件目录
			if (prop.getProperty("hdfsPath") == null) {
				logger.info("tmpPath property not exits in app.properties");
				System.exit(-1);
			}
			HDSSPATH = prop.getProperty("hdfsPath");

			// 文件解压线程数
			if (prop.getProperty("threadUntarNum") != null
					&& Integer.parseInt(prop.getProperty("threadUntarNum").trim()) <= 0) {
				logger.info("threadUntarNum illegal");
				System.exit(-1);
			}
			THREADUNTAR_NUM = Integer.parseInt(prop.getProperty("threadUntarNum"));

			// 文件上传线程数
			if (prop.getProperty("threadPutNum") != null
					&& Integer.parseInt(prop.getProperty("threadPutNum").trim()) <= 0) {
				logger.info("threadPutNum illegal");
				System.exit(-1);
			}
			THREADPUT_NUM = Integer.parseInt(prop.getProperty("threadPutNum"));

			// 初始化数据库连接
//			 in = WatchFile.class.getClassLoader().getResourceAsStream("dbcpconfig.properties");
//			 prop.load(in);

			File file2 = new File("dbcpconfig.properties");
			FileInputStream fis2 = new FileInputStream(file2);
			prop.load(fis2);
			fis2.close();

			String pass = Util.getFromBase64(prop.getProperty("password"));
			prop.remove(pass);
			prop.setProperty("password", pass);
			Util.ds = BasicDataSourceFactory.createDataSource(prop);

			// 初始化文件监听目录
			Path dir = Paths.get(RECEIVE_PATH);
			new WatchFile(dir, recursive).processEvents();

		} catch (Exception e) {
			logger.info("properties not exits or error");
			e.printStackTrace();
			System.exit(-1);
		}
	}
}

package watchfile.common;


public class Conmmon {
	public static String INIT_STATE = "00";	//初始化就绪文件状态
	public static String VERIFILE_NAME_INVALID = "001";	//校验文件名称不正确
	public static String VERIFILE_DDATE_INNALID = "002";	//就绪文件数据日期非法
	public static String VERIFILE_CANNOT_OPEN = "003";	//就绪文件无法打开
	public static String DATFILE_NAME_INVALID = "004";	//接口文件名与规则不符
	public static String DATFILE_NOT_EXIST = "005";	//接口数据文件不存在
	public static String DATFILE_CANNOT_OPEN = "006";	//接口数据文件无法打开
	public static String VERIFILEBYTAR_PUTTOTMP_SUCC = "11";	//MOVE就绪文件和TAR包到临时目录成功
	public static String VERIFILEBYTAR_PUTTOTMP_FAIL = "111";	//MOVE就绪文件和TAR包到临时目录失败
	public static String REC_LENGTH_ERROR = "112";	//记录长度错误
	public static String REC_NUM_ERROR = "113";	//文件记录数不符
	public static String DATFILE_CDATE_ERROR = "114";	//文件生成时间不符
	public static String DATFILE_DDATE_ERROR = "115";	//文件数据日期不符
	public static String DATFILE_LENGTH_ERROR = "116";	//文件大小不符
	public static String DATFILE_DDATE_INVALID = "117";	//数据文件数据日期非法
	public static String TAR_UNZIP_SUCC = "22";	//TAR包解压成功
	public static String TAR_UNZIP_FAIL = "221";	//TAR包解压失败
	public static String VERIFILEBYTAR_PUTOHDFS_SUCC = "33";	//文件PUT到HDFS目录成功
	public static String VERIFILEBYTAR_PUTOHDFS_FAIL = "331";	//文件PUT到HDFS目录失败

}
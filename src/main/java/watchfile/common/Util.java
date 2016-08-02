package watchfile.common;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.UnsupportedEncodingException;  
import sun.misc.*; 
/**
 * 通用函数类
 * @author Administrator
 *
 */
public class Util {
	public static SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss.SSS");
	public static SimpleDateFormat sdfymd = new SimpleDateFormat("YYYYMMdd");
	public static  String APP_NAME = "";
	public static DataSource ds = null;
	/**
	 * 解析xml文件
	 * @param filename
	 * @return
	 */
    public static Document parse(FileInputStream filename) {
        Document document = null;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            document = builder.parse(filename);
            document.normalize();
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return document;
    }
    /**
     * 目前支持年月日，年月日小时，年月日小时分钟
     * @param str
     * @return
     */
    public static boolean isValidDate(String str) {
        boolean convertSuccess=true;
         SimpleDateFormat format = null;
         if(str.length()==8){
        	 format = new SimpleDateFormat("yyyyMMdd");
         }
         if(str.length()==10){
        	 format = new SimpleDateFormat("yyyyMMddHH");
         }
         if(str.length()==12){
        	 format = new SimpleDateFormat("yyyyMMddHHmm");
         }
         try {
            format.setLenient(false);
            format.parse(str);
         } catch (Exception e) {
             convertSuccess=false;
         } 
         return convertSuccess;
  }
    
    /**
     *持久化操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static boolean messageDao(FileOperaLog flo){
    	boolean tag = true;
    	 String sql = "insert into file_opera_log  values(?,?,?,?,?,?,?,?,?,?)";
   		 Connection conn;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
			 st.setString(1, UUID.randomUUID().toString());
	   	     st.setString(2, APP_NAME);
	   	     st.setString(3, flo.getFilePath());
	   	     st.setString(4, flo.getChefileName());
	   	     st.setString(5, flo.getTarName());
	   	     st.setString(6, flo.getCuttState());
	   	     st.setString(7, flo.getCarryState());
	   	     st.setString(8, flo.getCreateTime());
	   	     st.setString(9, flo.getUpdateTime());
	   	     st.setString(10, flo.getStateremark());
	   	     st.executeUpdate();
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
			tag = false;
		}
   		 return tag;
    }
    
    
    /**
     *持久化操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static boolean controlDao(String xmlName,String dataFileName,String state){
    	 String sql = "insert into file_tar_log  values(?,?,?)";
   		 Connection conn;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
	   	     st.setString(1, xmlName);
	   	     st.setString(2,dataFileName);
	   	     st.setString(3,state);
	   	     st.executeUpdate();
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
		}
   		 return true;
    }
    
    /**
     *持久化更新操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static boolean controlUpdateDao(String xmlName,String dataFileName,String state){
    	 String sql = "update file_tar_log  set state=? where id = ? and file_name=? ";
   		 Connection conn;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
	   	     st.setString(1, state);
	   	     st.setString(2,xmlName);
	   	     st.setString(3,dataFileName);
	   	     st.executeUpdate();
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
		}
   		 return true;
    }
    
    
    /**
     *持久化更新操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static int controlSelectDao(String xmlName){
    	 String sql = "select sum(state) as sumstate  from file_tar_log   where id = ?";
   		 Connection conn;
   		ResultSet rs = null;
   		int re = 0;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
	   	     st.setString(1, xmlName);
	   	    rs=st.executeQuery();
	   	    if (rs.next()) {
	   	    	re=Integer.parseInt(rs.getString("sumstate"));
	   	    }
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
		}
   		 return re;
    }
    
    /**
     *持久化操作日志
     * @param fol
     * @param ds
     * @return
     */
    public static boolean updateMessageDao(FileOperaLog fol){
    	boolean tag = true;
    	String sql = "update file_opera_log  set  update_time=?,cutt_state=?,carry_state=?,state_remark=?  where chefile_name=? ";
   		 Connection conn;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
			 st.setString(1, sdf.format(new Date()));
			 st.setString(2,fol.getCuttState());
			 st.setString(3, fol.getCarryState());
			 st.setString(4, fol.getStateremark());
			 st.setString(5, fol.getChefileName());
			 
	   	     st.executeUpdate();
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
			tag = false;
		}
   		 return tag;
    	
    }
    
    /**
     *持久化操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static String isTarexit(FileOperaLog flo){
    	String tag = "0";
    	 String sql ="select chefile_name from file_opera_log where chefile_name ='"+flo.getChefileName()+"'";
   		 Connection conn;
   		 ResultSet rs = null;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  st = conn.prepareStatement(sql);
	   	     rs=st.executeQuery();
	   	     
	   	    if (rs.next()) {
	   	    	tag = "1";
	   	    }else{
	   	    	tag = "2";
	   	    }
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
			tag = "3";
		}
   		 return tag;
    }    
  
    
    /**
     * 检查文件大小是否合法
     * @param str
     * @return
     */
    public static boolean isNumeric(String str){
    	  for (int i = 0; i < str.length(); i++){
    	   if (!Character.isDigit(str.charAt(i))){
    	    return false;
    	   }
    	  }
    	  return true;
    }
    
    
    
    /**
     * 判断文件是否可打开
     * @param file
     * @return
     */
    public static boolean openFile(String file){
    	return (new File(file)).canRead();
    }
    
    
    /**
     * 释放链接
     * @param conn
     * @param st
     */
    public static void release(Connection conn,Statement st){
        if(st!=null){
            try{
                st.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        if(conn!=null){
            try{
                 
                conn.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 根据路径全称返回名字
     * @param pathName
     * @return
     */
    public static String pathFileName(String pathName){
    	if(pathName!=null&&pathName.indexOf(File.separator)>-1){
    		pathName = pathName.substring(pathName.lastIndexOf(File.separator)+1,pathName.length());
    	}
    	return   pathName;
    }
    
    /**
     * 根据路径全称返回名字
     * @param pathName
     * @return
     */
    public static String pathName(String pathName){
    	if(pathName!=null&&pathName.indexOf(File.separator)>-1){
    		pathName = pathName.substring(0,pathName.lastIndexOf(File.separator)+1);
    	}
    	return   pathName;
    	
    }
    
    
    // 加密  
    public static String getBase64(String str) {  
        byte[] b = null;  
        String s = null;  
        try {  
            b = str.getBytes("utf-8");  
        } catch (UnsupportedEncodingException e) {  
            e.printStackTrace();  
        }  
        if (b != null) {  
            s = new BASE64Encoder().encode(b);  
        }  
        return s;  
    }  
  
    // 解密  
    public static String getFromBase64(String s) {  
        byte[] b = null;  
        String result = null;  
        if (s != null) {  
            BASE64Decoder decoder = new BASE64Decoder();  
            try {  
                b = decoder.decodeBuffer(s);  
                result = new String(b, "utf-8");  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
        return result;  
    }
    
    /**
     *持久化操作日志
     * @param flo
     * @param ds
     * @return
     */
    public static boolean controlPidbyAppDao(String appName,String pid){
    	 String sql = "insert into app_pid  values(?,?)";
    	 String delsql = "delete from app_pid where app_name= ? ";
   		 Connection conn;
		try {
			 conn = ds.getConnection();
			 PreparedStatement  delst = conn.prepareStatement(delsql);
			 delst.setString(1, appName);
			 delst.executeUpdate();
			 
			 PreparedStatement  st = conn.prepareStatement(sql);
	   	     st.setString(1, appName);
	   	     st.setString(2,pid);
	   	     st.executeUpdate();
	   	     
	   	     release(conn, st);
		} catch (SQLException e) {
			e.printStackTrace();
		}
   		 return true;
    }
    
    
    public static void main(String[] args)   {  
//    	FileInputStream fis;
//		try {
//			fis = new FileInputStream(new File("E:\\tmp\\bdap_data_app1_20160630_001.xml"));
//			parse(fis);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	FileOperaLog flo = new FileOperaLog();
//    	flo.setChefileName("bdap_data_app1_20160630_001.xml");
    	Util.getBase64("mysql");
    	System.out.println("1-------------"+Util.getBase64("mysql"));
    }
}

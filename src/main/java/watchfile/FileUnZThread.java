package watchfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import org.apache.log4j.Logger;
import java.util.zip.GZIPInputStream;
import com.ice.tar.TarEntry;
import com.ice.tar.TarInputStream;

/**
 * 实现Z包多线程解压
 * @author Administrator
 *
 */
public class FileUnZThread implements Runnable {
	private static Logger logger = Logger.getLogger("Log");  
	private String fileName;
	private String untarDir;
    
    public FileUnZThread(String fileName,String untarDir){
        this.fileName=fileName;
        this.untarDir = untarDir;
    }
    
    @Override
    public void run() {
    	logger.info(Thread.currentThread().getName()+" unZ files  [ "+fileName + "]"); 
        processCommand(fileName,untarDir);
    }
    
    /**
     * 文件解压
     * @param fileName
     */
    private void processCommand(String tarFileName,String untarDir) {
    	InputStream in;
    	try{
    	if(tarFileName.substring(tarFileName.lastIndexOf(".") + 1, tarFileName.lastIndexOf(".") + 2).equalsIgnoreCase("Z")){
    		logger.info("Creating an GZIPInputStream for the file"); 
            in =  new GZIPInputStream(new FileInputStream(new File(tarFileName)));
         }else{
        	logger.info("Creating an InputStream for the file"); 
            in = new FileInputStream(new File(tarFileName));
         }  
    	 logger.info("Reading TarInputStream... "); 

         TarInputStream tin = new TarInputStream(in);
         TarEntry tarEntry = tin.getNextEntry();
         if(new File(untarDir).exists()){
             while (tarEntry != null){
                File destPath = new File(untarDir + File.separatorChar + tarEntry.getName());
           	    logger.info("Processing " + destPath.getAbsoluteFile()); 
                
                if(!tarEntry.isDirectory()){
                   FileOutputStream fout = new FileOutputStream(destPath);
                   tin.copyEntryContents(fout);
                   fout.close();
                }else{
                   destPath.mkdir();
                }
                tarEntry = tin.getNextEntry();
             }
             tin.close(); 
         }else{
        	 logger.info("That destination directory doesn't exist! " + untarDir);  
               } 
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
 
    @Override
    public String toString(){
        return this.fileName;
    }
}
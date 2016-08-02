package watchfile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.log4j.Logger;
import com.ice.tar.TarEntry;
import com.ice.tar.TarInputStream;

/**
 * 
 * @author Administrator
 *
 */
public class Untargz {
	private static Logger logger = Logger.getLogger("Log");

	public static InputStream getInputStream(String tarFileName) throws Exception {

		if (tarFileName.substring(tarFileName.lastIndexOf(".") + 1, tarFileName.lastIndexOf(".") + 2)
				.equalsIgnoreCase("Z")) {
			logger.info("Creating an GZIPInputStream for the file");
			System.out.println("Creating an GZIPInputStream for the file");
			return new GZIPInputStream(new FileInputStream(new File(tarFileName)));
		} else {
			logger.info("Creating an InputStream for the file");
			return new BufferedInputStream(new FileInputStream(new File(tarFileName)));
		}
	}

	private static List<String> untar(InputStream in, String untarDir) throws IOException {
		List<String> tar = new ArrayList<String>();
		logger.info("Reading TarInputStream... ");
		TarInputStream tin = new TarInputStream(in);
		TarEntry tarEntry = tin.getNextEntry();
		if (new File(untarDir).exists()) {
			while (tarEntry != null) {
				File destPath = new File(untarDir + File.separatorChar + tarEntry.getName());
				logger.info("Processing " + destPath.getAbsoluteFile());

				tar.add(destPath.getAbsoluteFile().toString());

				if (!tarEntry.isDirectory()) {
					FileOutputStream fout = new FileOutputStream(destPath);
					tin.copyEntryContents(fout);
					fout.close();
				} else {
					destPath.mkdir();
				}
				tarEntry = tin.getNextEntry();
			}
			tin.close();
			// 解压tar包成功后多线程方式解压.Z压缩包
		} else {
			logger.info("That destination directory doesn't exist! " + untarDir);
		}

		return tar;

	}

	public static HashMap run(String strSourceFile, String strDest) {
		HashMap res = new HashMap();
		List<String> tar = new ArrayList<String>();
		boolean re = true;
		try {
			InputStream in = getInputStream(strSourceFile);
			// 解压tar包
			tar = untar(in, strDest);
		} catch (Exception e) {
			re = false;
			e.printStackTrace();
			logger.info(e.getMessage());
		}
		res.put("mainFla", re);
		res.put("untar", tar);
		return res;
	}
}
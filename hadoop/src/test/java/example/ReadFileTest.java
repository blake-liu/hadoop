/**
 * Project Name:hadoop
 * File Name:ReadFile.java
 * Package Name:example
 * Date:2015年9月2日上午11:24:48
 *
*/

package example;

import java.io.IOException;

/**
 * ClassName:ReadFile <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * 调用：java -cp example-0.0.1-SNAPSHOT.jar example.ReadFileTest 
 * Date:     2015年9月2日 上午11:24:48 <br/>
 * @author   blake
 * @version  
 * @since    SHUNHE 1.0
 * @see 	 
 */
public class ReadFileTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		System.out.println("-------------------------------------------------");
		long curt = System.currentTimeMillis();
		String cmd = "Rscript -e \"fore <- doPOCForecast(c(" + "3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11" + ")," + "3" + ");write.csv(fore, file='" + "/tmp/poc/test1234" + "');\"";
		System.out.println("CMD:");
//		System.out.println(cmd);
		System.out.println("-------------------------------------------------");
//		Process p = rt.exec(cmd);
//		Process p = rt.exec("Rscript /opt/liuc/test.R");
		System.out.println("sh /opt/liuc/test.sh " + "3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11" + " " + " 3 " + "/tmp/poc/test12345");
		Process p = rt.exec("sh /opt/liuc/test.sh " + "3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11,3,8,4,8,4,5,11" + " " + " 3 " + "/tmp/poc/test12345");
		int code = p.waitFor();
		System.out.println("code:" + code);
		System.out.println("-------------------------------------------------");
		System.out.println("耗时：" + (System.currentTimeMillis() - curt));
		
	}
}


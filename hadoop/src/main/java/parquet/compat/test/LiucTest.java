/**
 * Project Name:hadoop
 * File Name:LiucTest.java
 * Package Name:parquet.compat.test
 * Date:2015年9月11日下午4:50:53
 *
*/

package parquet.compat.test;

import java.io.File;
import java.io.IOException;

/**
 * ClassName:LiucTest <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Date:     2015年9月11日 下午4:50:53 <br/>
 * @author   blake
 * @version  
 * @since    SHUNHE 1.0
 * @see 	 
 */
public class LiucTest {

//	public static void cloudera(){
//		// load your Avro schema
//		Schema avroSchema = new Schema.Parser().parse(in);
//		 
//		// generate the corresponding Parquet schema
//		MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
//		 
//		// create a WriteSupport object to serialize your Avro objects
//		AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
//		 
//		// choose compression scheme
//		compressionCodecName = CompressionCodecName.SNAPPY;
//		 
//		// set Parquet file block size and page size values
//		int blockSize = 256 * 1024 * 1024;
//		int pageSize = 64 * 1024;
//		 
//		Path outputPath = new Path(outputFilename);
//		 
//		// the ParquetWriter object that will consume Avro GenericRecords
//		parquetWriter = new ParquetWriter(outputPath,
//		        writeSupport, compressionCodecName, blockSize, pageSize);
//		 
//		for (GenericRecord record : SourceOfRecords) {
//		    parquetWriter.write(record);
//		}
//	}
	
	public static void main(String[] args) throws IOException {
		System.out.println(ConvertUtils.getSchema(new File("C:\\Users\\blake\\Desktop\\temp\\444\\customer.csv")));
		ConvertUtils.convertCsvToParquet(new File("C:\\Users\\blake\\Desktop\\temp\\444\\customer.csv"), 
				new File("C:\\Users\\blake\\Desktop\\temp\\444\\customer.parquet"));
//		System.out.println(ConvertUtils.getSchema(new File("D:\\工作\\铁科院\\drill性能测试\\数据\\liuc_catalog.csv")));
//		ConvertUtils.convertCsvToParquet(new File("D:\\工作\\铁科院\\drill性能测试\\数据\\liuc_catalog.csv"), new File("D:\\工作\\铁科院\\drill性能测试\\数据\\liuc_catalog.parquet"));
	}
}


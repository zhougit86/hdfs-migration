package avroTrial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by zhou1 on 2018/10/29.
 */
public class avroTrial {
    public static void main(String[] arg) throws IOException,URISyntaxException,Exception{
        if (arg.length !=4){
            throw new Exception("wrong arg number");
        }
        String destHdfs = arg[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(destHdfs),conf);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class,conf);

        String inputPath = arg[2];
        String outputPath = arg[3];

        if (arg[1].equals("zip")){
            String wareHouse = "/warehouse/ods/ods_hana/bic_azodw016300/";
            String schemaDir = ".schemas/";
            String avroDir = "sdt=20180716/";
            Path srcFile = new Path(inputPath);
            InputStream is = new BufferedInputStream(fs.open(srcFile));

            Path destFile = new Path(outputPath);
            OutputStream os = fs.create(destFile);
            CompressionOutputStream codecOut = codec.createOutputStream(os);

            IOUtils.copyBytes(is, codecOut, conf,true);

            is.close();
            codecOut.close();
            os.close();
        }else{
            Path srcFile = new Path(inputPath);
            InputStream is = fs.open(srcFile);
            CompressionInputStream codecIn = codec.createInputStream(is);


            Path destFile = new Path(outputPath);
            OutputStream os = fs.create(destFile);


            IOUtils.copyBytes(codecIn, os, conf,true);

            is.close();
            codecIn.close();
            os.close();
        }

    }
}

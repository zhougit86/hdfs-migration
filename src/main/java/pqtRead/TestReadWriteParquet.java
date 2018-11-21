package pqtRead;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.common.base.Charsets;
import compressFile.compressFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.parquet.Log;
import org.apache.parquet.Strings;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class TestReadWriteParquet  extends Configured implements Tool {
    private static final Log LOG = Log.getLog(TestReadWriteParquet.class);

    public static class MyfileOutputFormat extends TextOutputFormat<Void,Text> {
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        //获取文件时间

        public RecordWriter<Void, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            String keyValueSeparator = conf.get(SEPERATOR, "\t");
            CompressionCodec codec = null;
            String extension = "";
            if(isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }

            Path file = this.getDefaultWorkFile(job, extension);
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut;
            if(!isCompressed) {
                fileOut = fs.create(file, false);
                return new myRecordWriter(fileOut, keyValueSeparator);
            } else {
                fileOut = fs.create(file, false);
                return new myRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
            }
        }

//        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
//            FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
//            String JobPath = committer.getJobAttemptPath(context).toString();
//            JobPath = JobPath.substring(0,JobPath.indexOf("_temporary"));
//            LOG.info("*****yonghui*****:Output path: "+JobPath);
//
//            //下列函数第二个参数就是文件名
//            Path pathOfFile = new Path(JobPath, getUniqueFile(context, "000000_0", extension));
//            return pathOfFile;
//        }

        public static synchronized String getUniqueFile(TaskAttemptContext context, String name, String extension){
//            TaskID taskId = context.getTaskAttemptID().getTaskID();
//            int partition = taskId.getId();
            StringBuilder result = new StringBuilder();

            if (extension.length()==0){
                name = name.replace(".gz","");
                result.append(name);
                return result.toString();
            }

            result.append(name);
//            result.append('$');
//            result.append(NUMBER_FORMAT.format((long)partition));
            result.append(extension);
            return result.toString();
        }


    }

    public static class MyFileInputFormat extends TextInputFormat{
        public MyFileInputFormat() {
        }

        @Override
        public boolean isSplitable(JobContext context, Path filename){
            return false;
        }

        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
            byte[] recordDelimiterBytes = null;
            if(null != delimiter) {
                recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
            }
//            for (Byte b: recordDelimiterBytes
//                 ) {
//                System.out.println(b);
//            }

            return new myReader(recordDelimiterBytes);
        }

//        protected boolean isSplitable(JobContext context, Path file) {
//            CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(file);
//            return null == codec?true:codec instanceof SplittableCompressionCodec;
//        }
    }

    /*
     * Read a Parquet record, write a Parquet record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Text, Text, GroupWithFileName> {
        private RecordWriter<Void, Text> writer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit) inputSplit).getPath().toString();

            //获取时间
            Configuration conf = context.getConfiguration();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
            Date startTime = null;




            Path SplitPath = ((FileSplit) inputSplit).getPath();

//            System.out.println(SplitPath.toString());
//            System.out.println(FileInputFormat.getInputPaths(context)[0].toString());
            String inputDir = FileInputFormat.getInputPaths(context)[0].toString();
            if (inputDir.charAt(inputDir.length()-1)!='/'){
                inputDir+="/";
            }
            LOG.info("*****yonghui*****:Input path: "+inputDir);
            final String pathWithDir = fileName.replace(inputDir,"");

            //设置文件名

            //包括hdfsXXX/input,需要被裁剪的文件路径
//            final int appendLen = FileInputFormat.getInputPaths(context)[0].toString().length()+1;


            MyfileOutputFormat tof = new MyfileOutputFormat(){
                public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                    FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
                    String JobPath = committer.getJobAttemptPath(context).toString();
                    JobPath = JobPath.substring(0,JobPath.indexOf("_temporary"));
                    LOG.info("*****yonghui*****:Output path: "+JobPath);
                    String filename = getUniqueFile(context, pathWithDir, extension);


                    //下列函数第二个参数就是文件名
                    Path pathOfFile = new Path(JobPath,filename );
                    LOG.info("*****yonghui*****:Output file: "+pathOfFile);
                    return pathOfFile;
                }
            } ;
            writer = tof.getRecordWriter(context);

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //如果没有初始化就不用跑了
            if(writer==null){
                return;
            }
//            System.out.println(key);
//            System.out.println(value);
            try{
//                context.write(new Text("aaa"), value);
                writer.write(null, value);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            if(writer==null){
                return;
            }
            writer.close(context);
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length < 3) {
            LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE [compression]");
            return 1;
        }
        String DestHdfs = args[0];
        String inputFile = args[1];
        String outputFile = args[2];
        String compression = (args.length > 3) ? args[3] : "none";

//        // Find a file in case a directory was passed
//        FileSystem fileSys= FileSystem.get(new URI(DestHdfs),getConf());
//        if (fileSys.exists(new Path(DestHdfs+outputFile))){
//            LOG.info("the output already Exists");
//            System.exit(2);
//            return 1;
//        }

        Configuration conf = getConf();

        //设置压缩格式
//        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
//        conf.set("yonghui.compress","uncompress");
//        if(compression.equalsIgnoreCase("snappy")) {
//            codec = CompressionCodecName.SNAPPY;
//            conf.set("yonghui.compress","snappy");

//        LOG.info("Output compression: " + codec);

        //设置map的类和设置任务
        Job job = Job.getInstance(conf, "file Compress");
        job.setJobName(conf.get("yonghui.name"));

        job.setJarByClass(getClass());
//        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);

        FileSystem fileSys = FileSystem.get(new URI(DestHdfs),getConf());
        if (fileSys.exists(new Path(DestHdfs+outputFile))){
//            System.out.println("Deleting the output directory");
            fileSys.delete(new Path(DestHdfs+outputFile));
        }
//        FileStatus[] listStatus = fs.listStatus( new Path(args[1]));
//        filePartitioner.initMap(listStatus);
//      设置有多少个reduce任务  job.setNumReduceTasks(filePartitioner.initMap(listStatus));
//        job.setPartitionerClass(filePartitioner.class);


        //设置输入输出格式
//        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(MyFileInputFormat.class);

//        GroupWriteSupport.setSchema(schema, getConf());
//        ParquetOutputFormat.setValidation(getConf(),false);
//        ParquetOutputFormat.setEnableDictionary(job,false);


//        MyfileOutputFormat.setWriteSupportClass(job, MySupport.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        conf.set("yonghui.compress","uncompress");
        if(compression.equalsIgnoreCase("gzip")) {
//            codec = CompressionCodecName.GZIP;
            conf.set("yonghui.compress","gzip");
            MyfileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        }

        //todo 设置输入输出的路径
        FileInputFormat.setInputDirRecursive(job,true);
        FileInputFormat.setInputPaths(job, new Path(DestHdfs+inputFile));
        FileOutputFormat.setOutputPath(job, new Path(DestHdfs+outputFile));
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.framework.name","local");
//        conf.set("parquet.writer.version","v1");

        Date startTime = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        String stStr =sdf.format(startTime);
//        System.out.println(stStr);
//        System.out.println(sdf.parse(stStr));
        conf.set("yonghui.startTime",sdf.format(startTime));
        conf.set("yonghui.hdfs",args[0]);

        conf.set("yonghui.name","haha");
//        TestReadWriteParquet.setStartTime();
//        TestReadWriteParquet.initFileSys(args[0],conf);
//        TestReadWriteParquet.setFirstTime(true);
        try {
            int res = ToolRunner.run(conf, new TestReadWriteParquet(), args);
//            for(Map.Entry<String, String> entry: TestReadWriteParquet.map.entrySet()) {
//                System.out.println(entry);
//            }
            System.out.println(res);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }

    }
}



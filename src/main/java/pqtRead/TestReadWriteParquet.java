package pqtRead;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import compressFile.compressFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.parquet.Log;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

public class TestReadWriteParquet  extends Configured implements Tool {
    private static final Log LOG = Log.getLog(TestReadWriteParquet.class);

    public static class MyfileOutputFormat<T> extends ParquetOutputFormat<T> {
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        //获取文件时间
        private static FileSystem fSys = null;

        static class parseErr extends Exception{}
        private static Date getDateFromName(String fileName) throws parseErr {
            String[] fileNameParts = fileName.split("\\$");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

            if (fileNameParts.length<4){
                throw new parseErr();
            }
            try{
                return sdf.parse(fileNameParts[2]);
            }catch (ParseException e){
                LOG.error("*****yonghui*****:wrong output format."+fileName);
                throw new parseErr();
            }
        }
        private static String getOriginName(String fileName) throws parseErr {
            String[] fileNameParts = fileName.split("\\$");

            if (fileNameParts.length<4){
                throw new parseErr();
            }
            return fileNameParts[0];
        }
        public static void setFileSystem(String fsPath,Configuration conf){
            try{
                fSys = FileSystem.get(new URI(fsPath),conf);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        private CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
            return CodecConfig.from(taskAttemptContext).getCodec();
        }

        public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
            CompressionCodecName codec = this.getCodec(taskAttemptContext);
            String extension = codec.getExtension();
            Path file = this.getDefaultWorkFile(taskAttemptContext, extension);

            return this.getRecordWriter(conf, file, codec);
        }

        public static synchronized String getUniqueFile(TaskAttemptContext context, String name, String extension){
            TaskID taskId = context.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();

            StringBuilder result = new StringBuilder();
            result.append(name);
            result.append('$');
            result.append(NUMBER_FORMAT.format((long)partition));
            result.append(extension);
            return result.toString();
        }

        //actualLocation用来传入原先的位置
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension, String actualLocation ) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
            String JobPath = committer.getJobAttemptPath(context).toString();
            JobPath = JobPath.substring(0,JobPath.indexOf("_temporary"));
            LOG.info("*****yonghui*****:Output path: "+JobPath);

            Path pathOfFile = new Path(JobPath, getUniqueFile(context, actualLocation, extension));
            return pathOfFile;
        }
    }

    /*
     * Read a Parquet record, write a Parquet record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Group, Text, GroupWithFileName> {
        private RecordWriter<Void, Group> writer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit) inputSplit).getPath().toString();

            //获取时间
            Configuration conf = context.getConfiguration();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
            Date startTime = null;
            try{
                startTime = sdf.parse(conf.get("yonghui.startTime"));
            }catch (Exception e){
                e.printStackTrace();
            }
            boolean firstTime =  conf.get("yonghui.firstTime").equals("true")?true:false;

            //获取文件时间
            FileSystem fSys = null;
            try{
                fSys = FileSystem.get(new URI(conf.get("yonghui.hdfs")),conf);
            }catch (URISyntaxException e){
                e.printStackTrace();
            }


            Path SplitPath = ((FileSplit) inputSplit).getPath();
            FileStatus fStatus = fSys.getFileStatus(SplitPath);
            Date fileModTime = new Date(fStatus.getModificationTime());


            //获取schema和包含路径名的文件名
            ParquetMetadata readFooter;
            readFooter= ParquetFileReader.readFooter(context.getConfiguration(), SplitPath);
            final MessageType schema = readFooter.getFileMetaData().getSchema();
            //设置文件名
            final String pathWithDir;
            //包括hdfsXXX/input,需要被裁剪的文件路径
            final int appendLen = FileInputFormat.getInputPaths(context)[0].toString().length()+1;


            if(conf.get("yonghui.compress").equals("uncompress")){
                String pathWithoutInput = fileName.substring(appendLen,fileName.length()-3);
                pathWithDir = pathWithoutInput;

                //设置输出目录的文件结构
                MyfileOutputFormat.setFileSystem(conf.get("yonghui.hdfs"),conf);
            }else{
                LOG.info("*****yonghui*****:"+SplitPath+" modifytime: " + fileModTime.toString()+" starttime: " + startTime.toString());
                String pathWithoutInput = fileName.substring(appendLen);
                pathWithoutInput = pathWithoutInput.replace("$","");
                pathWithDir = pathWithoutInput
                        + "$" + conf.get("yonghui.startTime")
                        + "$" +sdf.format(fileModTime);

                //如果开始时间比较晚，且不是首次跑，就不用跑了,只有在压缩的时候有这个逻辑
                if(startTime.after(fileModTime) && !firstTime ){
                    LOG.info("*****yonghui*****:no need to setup");
                    writer = null;
                    return;
                }
            }

            MyfileOutputFormat<Group> tof = new MyfileOutputFormat<Group>() {
                private MySupport writeSupport;

                private CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
                    return CodecConfig.from(taskAttemptContext).getCodec();
                }
                @Override
                public RecordWriter<Void, Group> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                    Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
                    CompressionCodecName codec = this.getCodec(taskAttemptContext);
                    String extension = codec.getExtension() ;
                    Path file = this.getDefaultWorkFile(taskAttemptContext, extension, pathWithDir);
                    LOG.info("*****yonghui*****:Original Path: "+file);

                    if (extension.length() ==0){
                        String fileName = file.getName();

                        try{
                            String originFileName = super.getOriginName(fileName);
                            Date newDate = super.getDateFromName(fileName);
                            try{
                                FileStatus[] fileStatuses= super.fSys.listStatus(file.getParent());
                                for ( FileStatus f:fileStatuses) {
                                    String cmpFileName = f.getPath().toString();
                                    //截去前缀等信息
                                    cmpFileName = cmpFileName.substring(FileOutputFormat.getOutputPath(taskAttemptContext).toString().length()+1);

                                    Date cmpDate = super.getDateFromName(cmpFileName);
                                    String originCmpName = super.getOriginName(cmpFileName);
                                    if (!originFileName.equals(originCmpName)){
                                        continue;
                                    }
                                    if (newDate.after(cmpDate)){
                                        //删除所有的目标目录下时间较旧的同源文件
                                        LOG.info("need to delete the file:"+f.toString());
                                        super.fSys.delete(f.getPath(),true);
                                    }else{
                                        LOG.info("no later,give up");
                                        //只要发现时间是早于之前，直接放弃写文件
                                        return null;
                                    }
                                }
                            }catch (Exception e){
                                //如果path不存在就直接Writer返回就可以了
                                return this.getRecordWriter(conf, file, codec);
                            }
                        }catch (parseErr e){

                        }
                    }

                    return this.getRecordWriter(conf, file, codec);
                }
                public MySupport getWriteSupport(Configuration configuration) {
                    if(this.writeSupport != null) {
                        return this.writeSupport;
                    } else {
                        Class writeSupportClass = getWriteSupportClass(configuration);

                        try {
                            MySupport ms= (MySupport)((Class) Preconditions.checkNotNull(writeSupportClass, "writeSupportClass")).newInstance();
                            ms.setSchema(schema);
                            return ms;
                        } catch (InstantiationException var4) {
                            throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, var4);
                        } catch (IllegalAccessException var5) {
                            throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, var5);
                        }
                    }
                }
            };
            writer = tof.getRecordWriter(context);
        }

        @Override
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            //如果没有初始化就不用跑了
            if(writer==null){
                return;
            }
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
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        conf.set("yonghui.compress","uncompress");
        if(compression.equalsIgnoreCase("snappy")) {
            codec = CompressionCodecName.SNAPPY;
            conf.set("yonghui.compress","snappy");
        } else if(compression.equalsIgnoreCase("gzip")) {
            codec = CompressionCodecName.GZIP;
            conf.set("yonghui.compress","gzip");
        }
        LOG.info("Output compression: " + codec);

        //设置map的类和设置任务
        Job job = Job.getInstance(conf, "file Compress");
        job.setJobName(conf.get("yonghui.name"));

        job.setJarByClass(getClass());
//        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);

//        FileSystem fs = FileSystem.get(new URI(DestHdfs),getConf());
//        FileStatus[] listStatus = fs.listStatus( new Path(args[1]));
//        filePartitioner.initMap(listStatus);
//      设置有多少个reduce任务  job.setNumReduceTasks(filePartitioner.initMap(listStatus));
//        job.setPartitionerClass(filePartitioner.class);
        //设置输入输出格式
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(ParquetInputFormat.class);
//        GroupWriteSupport.setSchema(schema, getConf());
//        ParquetOutputFormat.setValidation(getConf(),false);
//        ParquetOutputFormat.setEnableDictionary(job,false);
        MyfileOutputFormat.setWriteSupportClass(job, MySupport.class);
        job.setOutputFormatClass(NullOutputFormat.class);


        MyfileOutputFormat.setCompression(job, codec);

        //todo 设置输入输出的路径
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
        conf.set("yonghui.firstTime","true");
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



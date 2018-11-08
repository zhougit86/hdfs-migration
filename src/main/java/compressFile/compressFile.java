package compressFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class taskFilenameMap{
    private final HashMap<String,String> taskFilenameMap = new HashMap();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock r = lock.readLock();
    private final Lock w = lock.writeLock();

    public taskFilenameMap(){}

    public void addElem(String task,String fName){
        w.lock();
        try{
            if (null == taskFilenameMap.get(task)){
                taskFilenameMap.put(task,fName);
            }
        }finally {
            w.unlock();
        }
    }

    public String getElem(String task){
        r.lock();
        try{
            return taskFilenameMap.get(task);
        }finally {
            r.unlock();
        }
    }
}

public class compressFile {

    private static final taskFilenameMap map = new  taskFilenameMap();

    public static class filePartitioner extends Partitioner<Text,Text>{
        private static int fileNumber = 0;
        public static HashMap<String,Integer> fileDist = new HashMap();
        public static int initMap(FileStatus[] listStatus){
            System.out.println("contains files:" + listStatus.length);

            for (FileStatus f: listStatus){
//                System.out.println("partxiaoxiao"+f.getLen()+f);
                if (f.getLen()==0){
                    continue;
                }
                String TaskId = String.valueOf(fileNumber);
                if (map.getElem(TaskId)==null){
                    String[] sliceList = f.getPath().toString().split("/");
                    String lastString = sliceList[sliceList.length-1];
                    System.out.println(TaskId +"____"+lastString);
                    map.addElem(TaskId,lastString);
                }
                fileDist.put(f.getPath().toString(),fileNumber++);
            }
            return fileNumber;
        }
        @Override
        public int getPartition(Text key,Text value,int numP){
            Integer fileId = fileDist.get(key.toString());
            return fileId==null?fileNumber:fileId;
        }
    }

    public static class textComparator extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return 1;
        }
    }

    public static class readerFileMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit) inputSplit).getPath().toString();
            context.write(new Text(fileName), value);
        }
    }

    public static class writeFileReducer
            extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text t: values){
                context.write(NullWritable.get(), t);
            }
        }
    }


    public static class MyfileOutputFormat extends TextOutputFormat<Text,Text>{
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        private static HashMap<String,FSDataOutputStream> fileDist = new HashMap();
        private static String inputDir;
        private static String outputDir;
//        private static URI destFs;

        public static void setOutputDir(String output){outputDir = output;}
        public static void setInputDir(String input){inputDir = input;}
//        public static void setHdfs(String destFs1) throws Exception{destFs = new URI(destFs1);}

        public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
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
            String TaskId = String.valueOf(job.getTaskAttemptID().getTaskID().getId());
//            System.out.println("Rw"+TaskId +"____"+map.getElem(TaskId));
            Path file = this.getDefaultWorkFile(job, extension,map.getElem(TaskId));
            FileSystem fs = file.getFileSystem(conf);
            FSDataOutputStream fileOut;
            if(!isCompressed) {
                fileOut = fs.create(file, false);
                return new TextOutputFormat.LineRecordWriter(fileOut, keyValueSeparator);
            } else {
                fileOut = fs.create(file, false);
                return new TextOutputFormat.LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
            }
        }

        public static synchronized String getUniqueFile(TaskAttemptContext context, String name, String extension) {
            TaskID taskId = context.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();
            StringBuilder result = new StringBuilder();
            result.append(name);
            result.append('-');
            result.append(NUMBER_FORMAT.format((long)partition));
            result.append(extension);
            return result.toString();
        }

        public Path getDefaultWorkFile(TaskAttemptContext context, String extension,String selfName) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
            return new Path(committer.getWorkPath(), getUniqueFile(context, selfName, extension));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        if (args.length !=4){
//            throw new Exception("need 4 args");
//        }
        String DestHdfs = args[0];
        String InputDir = args[0]+args[1];
        String OutputDir = args[0]+args[2];
        //todo:判断不能写入到input的DIR当中去
        System.out.println(DestHdfs);
        System.out.println(InputDir);
        System.out.println(OutputDir);
        Path inputPath = new Path(InputDir);
        Path outputPath = new Path(OutputDir);
        compressFile.MyfileOutputFormat.setInputDir(args[1]);
        compressFile.MyfileOutputFormat.setOutputDir(args[2]);

        Job job = Job.getInstance(conf, "file Compress");
        job.setJarByClass(compressFile.class);

//        //=====================================================
        FileSystem fs = FileSystem.get(new URI(DestHdfs),conf);
        FileStatus[] listStatus = fs.listStatus( new Path(args[1]));
        job.setNumReduceTasks(filePartitioner.initMap(listStatus));
//        //=====================================================

        //如果output存在先删除
        FileSystem outFs = outputPath.getFileSystem(conf);
        if (outFs.exists(outputPath)){
            System.out.println("the output already Exists");
            outFs.delete(outputPath,true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(compressFile.readerFileMapper.class);
        job.setPartitionerClass(compressFile.filePartitioner.class);
        job.setMapOutputKeyClass(Text.class);

        job.setReducerClass(compressFile.writeFileReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        //map排序
        job.setSortComparatorClass(textComparator.class);
        //reduce排序
        job.setGroupingComparatorClass(textComparator.class);

        if (args[3].equals("zip")){
            FileOutputFormat.setCompressOutput(job, true);  //job使用压缩
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式
        }
//        job.setOutputFormatClass(MyfileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

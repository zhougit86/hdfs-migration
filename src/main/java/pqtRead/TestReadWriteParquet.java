package pqtRead;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.parquet.Log;


public class TestReadWriteParquet  extends Configured implements Tool {
    private static final Log LOG = Log.getLog(TestReadWriteParquet.class);

//    public static class MyFilter implements PathFilter{
//        public boolean accept(Path var1){
//            return true;
//        }
//    }

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

        private static class MultiPathFilter implements PathFilter {
            private List<PathFilter> filters;

            public MultiPathFilter(List<PathFilter> filters) {
                this.filters = filters;
            }

            public boolean accept(Path path) {
                Iterator i$ = this.filters.iterator();

                PathFilter filter;
                do {
                    if(!i$.hasNext()) {
                        return true;
                    }

                    filter = (PathFilter)i$.next();
                } while(filter.accept(path));

                return false;
            }
        }

        protected List<FileStatus> listStatus(JobContext job) throws IOException {
            Path[] dirs = getInputPaths(job);
            if(dirs.length == 0) {
                throw new IOException("No input paths specified in job");
            } else {
                TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());
                boolean recursive = getInputDirRecursive(job);
                List<PathFilter> filters = new ArrayList();
//                filters.add(hiddenFileFilter);
                PathFilter jobFilter = getInputPathFilter(job);
                if(jobFilter != null) {
                    filters.add(jobFilter);
                }

                PathFilter inputFilter = new MultiPathFilter(filters);
                List<FileStatus> result = null;
                int numThreads = job.getConfiguration().getInt("mapreduce.input.fileinputformat.list-status.num-threads", 1);
                Stopwatch sw = (new Stopwatch()).start();
                if(numThreads == 1) {
                    result = this.singleThreadedListStatus(job, dirs, inputFilter, recursive);
                } else {
                    Iterable locatedFiles = null;

                    try {
                        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(job.getConfiguration(), dirs, recursive, inputFilter, true);
                        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
                    } catch (InterruptedException var12) {
                        throw new IOException("Interrupted while getting file statuses");
                    }

                    result = Lists.newArrayList(locatedFiles);
                }

                sw.stop();
//                if(LOG.isDebugEnabled()) {
//                    LOG.debug("Time taken to get FileStatuses: " + sw.elapsedMillis());
//                }

                LOG.info("Total input paths to process : " + ((List)result).size());
                return (List)result;
            }
        }

        private List<FileStatus> singleThreadedListStatus(JobContext job, Path[] dirs, PathFilter inputFilter,
                                                          boolean recursive) throws IOException {
            List<FileStatus> result = new ArrayList();
            List<IOException> errors = new ArrayList();

            for(int i = 0; i < dirs.length; ++i) {
                Path p = dirs[i];
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                FileStatus[] matches = fs.globStatus(p, inputFilter);
                if(matches == null) {
                    errors.add(new IOException("Input path does not exist: " + p));
                } else if(matches.length == 0) {
                    errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
                } else {
                    FileStatus[] arr$ = matches;
                    int len$ = matches.length;

                    label57:
                    for(int i$ = 0; i$ < len$; ++i$) {
                        FileStatus globStat = arr$[i$];
                        if(!globStat.isDirectory()) {
                            result.add(globStat);
                        } else {
                            RemoteIterator iter = fs.listLocatedStatus(globStat.getPath());

                            while(true) {
                                while(true) {
                                    LocatedFileStatus stat;
                                    do {
                                        if(!iter.hasNext()) {
                                            continue label57;
                                        }

                                        stat = (LocatedFileStatus)iter.next();
                                    } while(!inputFilter.accept(stat.getPath()));

                                    if(recursive && stat.isDirectory()) {
                                        this.addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                                    } else {
                                        result.add(stat);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if(!errors.isEmpty()) {
                throw new InvalidInputException(errors);
            } else {
                return result;
            }
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
//            Configuration conf = context.getConfiguration();
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
//            Date startTime = null;
//
//
//            Path SplitPath = ((FileSplit) inputSplit).getPath();

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

        //设置map的类和设置任务，设置由外部控制程序订的任务名
        Job job = Job.getInstance(conf, "file Compress");
        job.setJobName(conf.get("yonghui.name"));

        //不需要reduce的任务，将其设置为0
        job.setJarByClass(getClass());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);

        FileSystem fileSys = FileSystem.get(new URI(DestHdfs),getConf());
        if (!fileSys.exists(new Path(DestHdfs+inputFile))){
            fileSys.close();
            //如果输入文件路径不存在则直接返回
            return 1;
        }
        if (fileSys.exists(new Path(DestHdfs+outputFile))){
            fileSys.delete(new Path(DestHdfs+outputFile));
        }

        //设置输入输出格式
        job.setInputFormatClass(MyFileInputFormat.class);
        //不用设置outputFormat，直接在map任务中输出
        job.setOutputFormatClass(NullOutputFormat.class);

        conf.set("yonghui.compress","uncompress");
        if(compression.equalsIgnoreCase("gzip")) {
            conf.set("yonghui.compress","gzip");
            MyfileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        }

        //设置递归遍历，设置输入输出的路径
        FileInputFormat.setInputDirRecursive(job,true);
        FileInputFormat.setInputPaths(job, new Path(DestHdfs+inputFile));
//        FileInputFormat.setInputPathFilter(job,MyFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(DestHdfs+outputFile));
        fileSys.close();
        if (job.waitForCompletion(true)){
            return 0;
        }else {
            //未知错误
            return 10;
        }
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
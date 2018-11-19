package pqtRead;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class myRecordWriter<K, V> extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public myRecordWriter(DataOutputStream out, String keyValueSeparator) {
        this.out = out;

        try {
            this.keyValueSeparator = keyValueSeparator.getBytes("UTF-8");
        } catch (UnsupportedEncodingException var4) {
            throw new IllegalArgumentException("can't find UTF-8 encoding");
        }
    }

    public myRecordWriter(DataOutputStream out) {
        this(out, "\t");
    }

    private void writeObject(Object o) throws IOException {
        if(o instanceof Text) {
            Text to = (Text)o;
            this.out.write(to.getBytes(), 0, to.getLength());
        } else {
            this.out.write(o.toString().getBytes("UTF-8"));
        }

    }

    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if(!nullKey || !nullValue) {
            if(!nullKey) {
                this.writeObject(key);
            }

            if(!nullKey && !nullValue) {
                this.out.write(this.keyValueSeparator);
            }

            if(!nullValue) {
                this.writeObject(value);
            }

//            this.out.write(newline);
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }

    static {
        try {
            newline = "\n".getBytes("UTF-8");
        } catch (UnsupportedEncodingException var1) {
            throw new IllegalArgumentException("can't find UTF-8 encoding");
        }
    }
}
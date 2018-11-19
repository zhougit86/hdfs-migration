package pqtRead;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

@Private
@Unstable
public class myUncompressedSplitReader extends mySplitLineReader {
    private boolean needAdditionalRecord = false;
    private long splitLength;
    private long totalBytesRead = 0L;
    private boolean finished = false;
    private boolean usingCRLF;

    public myUncompressedSplitReader(FSDataInputStream in, Configuration conf, byte[] recordDelimiterBytes, long splitLength) throws IOException {
        super(in, conf, recordDelimiterBytes);
        this.splitLength = splitLength;
        this.usingCRLF = recordDelimiterBytes == null;
    }

    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter) throws IOException {
        int maxBytesToRead = buffer.length;
        if(this.totalBytesRead < this.splitLength) {
            System.out.println("hahahahahah");
            long leftBytesForSplit = this.splitLength - this.totalBytesRead;
            if(leftBytesForSplit <= 2147483647L) {
                maxBytesToRead = Math.min(maxBytesToRead, (int)leftBytesForSplit);
            }
        }

        int bytesRead = in.read(buffer, 0, maxBytesToRead);

        if(this.totalBytesRead == this.splitLength && inDelimiter && bytesRead > 0) {
            if(this.usingCRLF) {
                this.needAdditionalRecord = buffer[0] != 10;
            } else {
                this.needAdditionalRecord = true;
            }
        }
        System.out.printf("*$*$*$*$*$*$*$*$*$*$*$*$*$*$*$*$%b,%d\n",inDelimiter ,maxBytesToRead );
//        System.out.println(new String(buffer)+"\n\n");
        if(bytesRead > 0) {
            this.totalBytesRead += (long)bytesRead;
        }

        return bytesRead;
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        int bytesRead = 0;
        if(!this.finished) {
            System.out.printf("----------------totalRead:%d,splitLen:%d-------------\n",this.totalBytesRead,this.splitLength);
            if(this.totalBytesRead > this.splitLength) {
                this.finished = true;
            }

            bytesRead = super.simpleReadLine(str,maxLineLength,maxBytesToConsume);
//            bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
//            System.out.println(str);
        }

        return bytesRead;
    }

    public boolean needAdditionalRecordAfterSplit() {
        return !this.finished && this.needAdditionalRecord;
    }

    protected void unsetNeedAdditionalRecordAfterSplit() {
        this.needAdditionalRecord = false;
    }
}

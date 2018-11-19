package pqtRead;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

@Private
@Unstable
public class myCompressedSplitReader extends mySplitLineReader {
    SplitCompressionInputStream scin;
    private boolean usingCRLF;
    private boolean needAdditionalRecord = false;
    private boolean finished = false;

    public myCompressedSplitReader(SplitCompressionInputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        super(in, conf, recordDelimiterBytes);
        this.scin = in;
        this.usingCRLF = recordDelimiterBytes == null;
    }

    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter) throws IOException {
        int bytesRead = in.read(buffer);
        if(inDelimiter && bytesRead > 0) {
            if(this.usingCRLF) {
                this.needAdditionalRecord = buffer[0] != 10;
            } else {
                this.needAdditionalRecord = true;
            }
        }

        return bytesRead;
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        int bytesRead = 0;
        if(!this.finished) {
            if(this.scin.getPos() > this.scin.getAdjustedEnd()) {
                this.finished = true;
            }

//            bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
            bytesRead = super.simpleReadLine(str,maxLineLength,maxBytesToConsume);
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

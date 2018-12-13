package pqtRead;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

@LimitedPrivate({"MapReduce"})
@Unstable
public class myLineReader implements Closeable {
    private static final int DEFAULT_BUFFER_SIZE = 65536;
    private int bufferSize;
    private InputStream in;
    private byte[] buffer;
    private int bufferLength;
    private int bufferPosn;
    private static final byte CR = 13;
    private static final byte LF = 10;
    private final byte[] recordDelimiterBytes;

    public myLineReader(InputStream in) {
        this(in, 65536);
    }

    public myLineReader(InputStream in, int bufferSize) {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = null;
    }

    public myLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt("io.file.buffer.size", 65536));
    }

    public myLineReader(InputStream in, byte[] recordDelimiterBytes) {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.in = in;
        this.bufferSize = 65536;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    public myLineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes) {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    public myLineReader(InputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.in = in;
        this.bufferSize = conf.getInt("io.file.buffer.size", 65536);
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    public void close() throws IOException {
        this.in.close();
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
//        System.out.println("reading compress file");
        return this.simpleReadLine(str,maxLineLength,maxBytesToConsume);
//        return this.recordDelimiterBytes != null?this.readCustomLine(str, maxLineLength, maxBytesToConsume):this.readDefaultLine(str, maxLineLength, maxBytesToConsume);
    }

    public int simpleReadLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException{
        str.clear();
        int ret = fillBuffer(this.in, this.buffer,false);
//        System.out.println(new String(this.buffer));
        if (ret>=0){
            str.append(this.buffer,0,ret);
            return ret;
        }else{
            return 0;
        }
    }

    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter) throws IOException {
        return in.read(buffer);
    }

    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        int txtLength = 0;
        int newlineLength = 0;
        boolean prevCharCR = false;
        long bytesConsumed = 0L;

        do {
            int startPosn = this.bufferPosn;
            if(this.bufferPosn >= this.bufferLength) {
                startPosn = this.bufferPosn = 0;
                if(prevCharCR) {
                    ++bytesConsumed;
                }

                this.bufferLength = this.fillBuffer(this.in, this.buffer, prevCharCR);

                if(this.bufferLength <= 0) {
                    break;
                }
            }

            while(this.bufferPosn < this.bufferLength) {
                if(this.buffer[this.bufferPosn] == 10) {
                    newlineLength = prevCharCR?2:1;
                    ++this.bufferPosn;
                    break;
                }

                if(prevCharCR) {
                    newlineLength = 1;
                    break;
                }

                prevCharCR = this.buffer[this.bufferPosn] == 13;
                ++this.bufferPosn;
            }

            int readLength = this.bufferPosn - startPosn;
            if(prevCharCR && newlineLength == 0) {
                --readLength;
            }

            bytesConsumed += (long)readLength;
            int appendLength = readLength - newlineLength;
            if(appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }

            if(appendLength > 0) {
                str.append(this.buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while(newlineLength == 0 && bytesConsumed < (long)maxBytesToConsume);

        if(bytesConsumed > 2147483647L) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        } else {
            return (int)bytesConsumed;
        }
    }

    private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        int txtLength = 0;
        long bytesConsumed = 0L;
        int delPosn = 0;
        int ambiguousByteCount = 0;

        do {
            int startPosn = this.bufferPosn;
            if(this.bufferPosn >= this.bufferLength) {
                startPosn = this.bufferPosn = 0;
                this.bufferLength = this.fillBuffer(this.in, this.buffer, ambiguousByteCount > 0);
                if(this.bufferLength <= 0) {
                    if(ambiguousByteCount > 0) {
                        str.append(this.recordDelimiterBytes, 0, ambiguousByteCount);
                        bytesConsumed += (long)ambiguousByteCount;
                    }
                    break;
                }
            }

            for(; this.bufferPosn < this.bufferLength; ++this.bufferPosn) {
                if(this.buffer[this.bufferPosn] == this.recordDelimiterBytes[delPosn]) {
                    ++delPosn;
                    if(delPosn >= this.recordDelimiterBytes.length) {
                        ++this.bufferPosn;
                        break;
                    }
                } else if(delPosn != 0) {
                    this.bufferPosn -= delPosn;
                    if(this.bufferPosn < -1) {
                        this.bufferPosn = -1;
                    }

                    delPosn = 0;
                }
            }

            int readLength = this.bufferPosn - startPosn;
            bytesConsumed += (long)readLength;
            int appendLength = readLength - delPosn;
            if(appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }

            bytesConsumed += (long)ambiguousByteCount;
            if(appendLength >= 0 && ambiguousByteCount > 0) {
                str.append(this.recordDelimiterBytes, 0, ambiguousByteCount);
                ambiguousByteCount = 0;
                this.unsetNeedAdditionalRecordAfterSplit();
            }

            if(appendLength > 0) {
                str.append(this.buffer, startPosn, appendLength);
                txtLength += appendLength;
            }

            if(this.bufferPosn >= this.bufferLength && delPosn > 0 && delPosn < this.recordDelimiterBytes.length) {
                ambiguousByteCount = delPosn;
                bytesConsumed -= (long)delPosn;
            }
        } while(delPosn < this.recordDelimiterBytes.length && bytesConsumed < (long)maxBytesToConsume);

        if(bytesConsumed > 2147483647L) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        } else {
            return (int)bytesConsumed;
        }
    }

    public int readLine(Text str, int maxLineLength) throws IOException {
        return this.readLine(str, maxLineLength, 2147483647);
    }

    public int readLine(Text str) throws IOException {
        return this.readLine(str, 2147483647, 2147483647);
    }

    protected int getBufferPosn() {
        return this.bufferPosn;
    }

    protected int getBufferSize() {
        return this.bufferSize;
    }

    protected void unsetNeedAdditionalRecordAfterSplit() {
    }
}
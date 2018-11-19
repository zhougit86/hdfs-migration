package pqtRead;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;


@Private
@Unstable
public class mySplitLineReader extends myLineReader {
    public mySplitLineReader(InputStream in, byte[] recordDelimiterBytes) {
        super(in, recordDelimiterBytes);
    }

    public mySplitLineReader(InputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        super(in, conf, recordDelimiterBytes);
    }

    public boolean needAdditionalRecordAfterSplit() {
        return false;
    }
}

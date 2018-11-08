package pqtRead;

import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class MySupport extends WriteSupport<GroupWithFileName> {
    public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";
    private MessageType schema;
    private GroupWriter groupWriter;

    public MySupport() {
    }

    public static void setSchema(MessageType schema, Configuration configuration) {
        configuration.set("parquet.example.schema", schema.toString());
    }

    public static MessageType getSchema(Configuration configuration) {
        return MessageTypeParser.parseMessageType((String)Preconditions.checkNotNull(configuration.get("parquet.example.schema"), "parquet.example.schema"));
    }

    public WriteContext init(Configuration configuration) {
        this.schema = getSchema(configuration);
        return new WriteContext(this.schema, new HashMap());
    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.groupWriter = new GroupWriter(recordConsumer, this.schema);
    }

    public void write(GroupWithFileName record) {
        this.groupWriter.write(record.GetGroup());
    }
}
package cn.carl.hadoop.wrapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>Title: cn.carl.hadoop.wrapper FileOutputFormatWrapper</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2018/1/15 18:29
 * @Version 1.0
 */
public class FileOutputFormatWrapper<K, V> extends FileOutputFormat<K, V> {

    public FileOutputFormatWrapper() {
        super();
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
        super.checkOutputSpecs(job);
    }

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        return super.getDefaultWorkFile(context, extension);
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return super.getOutputCommitter(context);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}

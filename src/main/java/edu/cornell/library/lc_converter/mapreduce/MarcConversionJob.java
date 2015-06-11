package edu.cornell.library.lc_converter.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import edu.cornell.library.lc_converter.input_formats.TWPInputFormat;


public class MarcConversionJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(getClass());
    job.setJobName(getClass().getSimpleName());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

//    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    job.setInputFormatClass(TWPInputFormat.class);
//    job.setInputFormatClass(WholeFileInputFormat.class);
    FileInputFormat.setMaxInputSplitSize(job,20);

    job.setMapperClass(MarcConversionMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(TextOutputFormat.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new MarcConversionJob(), args);
    System.exit(rc);
  }
}

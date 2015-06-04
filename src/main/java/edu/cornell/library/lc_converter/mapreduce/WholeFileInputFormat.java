package edu.cornell.library.lc_converter.mapreduce;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;


public class WholeFileInputFormat extends FileInputFormat {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  /**
   * Creates a NonSplittableRecordReader to read each file assigned to this InputSplit.
   * Note, that unlike ordinary InputSplits, split must be a CombineFileSplit, and therefore
   * is expected to specify multiple files.
   *
   * @param split The InputSplit to read.  Throws an IllegalArgumentException if this is
   *        not a CombineFileSplit.
   * @param context The context for this task.
   * @return a CombineFileRecordReader to process each file in split.
   *         It will read each file with a WholeFileRecordReader.
   * @throws IOException if there is an error.
   */
  @Override
  public RecordReader<NullWritable, Text> createRecordReader(
          InputSplit split, TaskAttemptContext context) throws IOException {
    return new RecordReader<NullWritable, Text>( split, context, WholeFileRecordReader.class);
  }

//  @Override
//  public RecordReader<NullWritable, Text> createRecordReader(
//          InputSplit split, TaskAttemptContext context) throws IOException {
//
//    if (!(split instanceof CombineFileSplit)) {
//      throw new IllegalArgumentException("split must be a CombineFileSplit");
//    }
//    return new CombineFileRecordReader<NullWritable, Text>((CombineFileSplit) split, context, WholeFileRecordReader.class);
//  }

}






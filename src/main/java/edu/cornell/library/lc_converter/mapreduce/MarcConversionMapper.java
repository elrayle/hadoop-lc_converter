package edu.cornell.library.lc_converter.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.InterruptedException;
import java.io.IOException;
import java.lang.String;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;

//import edu.cornell.library.lc_converter.input_formats.TextWithPath;


public class MarcConversionMapper extends Mapper<LongWritable, Text, Text, Text>{
//public class MarcConversionMapper extends Mapper<LongWritable, TextWithPath, Text, IntWritable>{
//
//  /**
//   * Only override `run` instead of `map` method; because we just want to see one output
//   * per mapper, instead of printing every line.
//   */
//  @Override
//  public void run(Context context) throws IOException, InterruptedException{
//    context.nextKeyValue();
//    TextWithPath twp = context.getCurrentValue();
//    context.write(new Text(twp.getPath().toString()), new IntWritable(1));
//  }
//}


  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    Iterator marcRecords = marcxmlCollectionParser( marcxmlCollection.toString() ).iterator();

//    for( String marcXml = (String)marcRecords.next(); marcRecords.hasNext(); marcXml = (String)marcRecords.next() ){
//        context.write(new Text(marcXml), null);
//    }


//    Path filePath = ((FileSplit) context.getInputSplit()).getPath();
//    String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();

    // HARDCODE FOR NOW
    String filePathString = "hdfs://localhost:9000/user/hadoop/"+value;



//    context.write(marcxmlCollection, null);
    context.write(new Text("key="+key.toString()+", value="+value+", filePathString="+filePathString), null);

  }

//  private HashSet<String> marcxmlCollectionParser( String marcxmlCollection ) throws XMLStreamException, Exception {
//    InputStream xmlstream = new ByteArrayInputStream(marcxmlCollection.getBytes(StandardCharsets.UTF_8));
//    HashSet<String> marcRecords = new HashSet();
//    XMLInputFactory input_factory = XMLInputFactory.newInstance();
//    XMLStreamReader r  =
//            input_factory.createXMLStreamReader(xmlstream);
//    while (r.hasNext()) {
//      if (r.next() == XMLEvent.START_ELEMENT)
//        if (r.getLocalName().equals("record")) {
//          marcRecords.add(r.getText());
//        }
//    }
//    return marcRecords;
//  }

}


package edu.cornell.library.lc_converter.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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


public class MarcConversionMapper <K> extends Mapper<K, Text, Text, Text>{

  @Override
  protected void map(K unused, Text marcxmlCollection, Context context) throws IOException, InterruptedException {
//    Iterator marcRecords = marcxmlCollectionParser( marcxmlCollection.toString() ).iterator();

//    for( String marcXml = (String)marcRecords.next(); marcRecords.hasNext(); marcXml = (String)marcRecords.next() ){
//        context.write(new Text(marcXml), null);
//    }

      context.write(marcxmlCollection);

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
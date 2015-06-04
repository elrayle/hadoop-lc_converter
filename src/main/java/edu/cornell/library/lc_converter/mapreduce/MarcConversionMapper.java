package edu.cornell.library.lc_converter.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.io.InputStream;
import java.xml.stream.XMLStreamException;
import java.xml.stream.XMLStreamReader;
import java.xml.stream.XMLInputFactory;


public class MarcConversionMapper extends Mapper<Text, Text, Text, LongWritable> {

  @Override
  protected void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
    String urlString = urlText.toString();
    InputStream is = getUrl( urlString  );
    HashSet<String> marcRecords = marcxmlCollectionParser( is ).iterator();

    for( String marcXml = marcRecords.next(); marcRecords.hasNext(); marcXml = marcRecords.next() ){
        context.write(marcXml, null);
    }
  }

  private HashSet<String> marcxmlCollectionParser( InputStream xmlstream ) throws XMLStreamException, Exception {
    HashSet<String> marcRecords = new HashSet();
    XMLInputFactory input_factory = XMLInputFactory.newInstance();
    XMLStreamReader r  =
            input_factory.createXMLStreamReader(xmlstream);
    while (r.hasNext()) {
      if (r.next() == XMLEvent.START_ELEMENT)
        if (r.getLocalName().equals("record")) {
          marcRecords.add(r);
        }
    }
    return marcRecords;
  }

}
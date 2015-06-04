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
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.XMLEvent;


public class MarcConversionMapper <K> extends Mapper<K, Text, Text, Text>{

  @Override
  protected void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
    String urlString = urlText.toString();
    InputStream is = getUrl( urlString  );
    HashSet<String> marcRecords = marcxmlCollectionParser( is ).iterator();

    for( String marcXml = marcRecords.next(); marcRecords.hasNext(); marcXml = marcRecords.next() ){
        context.write(new Text(marcXml), null);
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
          marcRecords.add(r.getText());
        }
    }
    return marcRecords;
  }

  private InputStream getUrl(String url) throws IOException {
    InputStream is = null;
    try {
      is = davService.getFileAsInputStream(url);
      if( url.endsWith( ".gz" ) || url.endsWith(".gzip"))
        return new GZIPInputStream( is );
      else
        return is;
    } catch (Exception e) {
      throw new IOException("Could not get " + url , e);
    }
  }

}
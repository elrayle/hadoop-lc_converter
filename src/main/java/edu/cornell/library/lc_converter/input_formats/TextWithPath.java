package edu.cornell.library.lc_converter.input_formats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TextWithPath extends Text {
  private Path path;

  public TextWithPath(Path path){
    super();
    this.path = path;
  }

  public Path getPath(){
    return path;
  }
}

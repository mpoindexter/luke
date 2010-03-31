package org.getopt.luke.decoders;

import org.apache.lucene.document.NumberTools;

public class OldNumberToolsDecoder implements Decoder {

  @Override
  public String decodeTerm(String fieldName, Object value) throws Exception {
    return Long.toString(NumberTools.stringToLong(value.toString()));
  }
  
  @Override
  public String decodeStored(String fieldName, Object value) throws Exception {
    return decodeTerm(fieldName, value);
  }
  
  public String toString() {
    return "(old NumberTools)";
  }
  
}

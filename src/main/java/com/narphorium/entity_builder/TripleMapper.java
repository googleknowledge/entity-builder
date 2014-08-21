package com.narphorium.entity_builder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.utils.JsonUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class TripleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

  private NTriplesParser tripleParser = new NTriplesParser();
  private EntityFrame entityFrame = new EntityFrame();

  @Override
  public void configure(JobConf conf) {
    Path framePath = new Path(conf.get("frame-file"));
    try {
      FileSystem fs = FileSystem.get(conf);
      if (framePath != null) {
        entityFrame.parse(fs.open(framePath));
      }
    } catch (IOException ex) {
      Logger.getGlobal().severe(ex.toString());
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object,
   * org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void map(LongWritable lineNumber, Text rawTriple, OutputCollector<Text, Text> triples,
      Reporter reporter) throws IOException {
    //System.out.println("Triple Test");
    RdfTriple triple = tripleParser.parse(rawTriple.toString());

    // Only map triples that are needed for the frame
    if (entityFrame.getMappedPredicates().contains(triple.getPredicate())) {
      if (triple.getPredicate().equals(RDF_TYPE)) {
        triple = new RdfTriple(triple.getSubject(), "@type", triple.getObject());
      }
      triples.collect(new Text(triple.getSubject()), new Text(tripleAsJsonLd(triple)));
    }

    // Map reversed triples if they're needed to join nested entities together.
    String reversePredicate = "!" + triple.getPredicate();
    if (entityFrame.getMappedPredicates().contains(reversePredicate)) {
      RdfTriple reversedTriple =
          new RdfTriple(triple.getObject(), reversePredicate, triple.getSubject());
      triples.collect(new Text(triple.getObject()), new Text(tripleAsJsonLd(reversedTriple)));
    }
  }

  private String tripleAsJsonLd(RdfTriple triple) throws JsonGenerationException, IOException {
    Map<String, Object> jsonValue = new HashMap<String, Object>();
    jsonValue.put("@id", triple.getSubject());
    List<Object> objectValues = new ArrayList<Object>();
    if (triple.isLiteral() || triple.getPredicate().equals("@type")) {
      objectValues.add(triple.getObject());
    } else {
      Map<String, Object> valueObject = new HashMap<String, Object>();
      valueObject.put("@id", triple.getObject());
      objectValues.add(valueObject);
    }

    jsonValue.put(triple.getPredicate(), objectValues);
    return JsonUtils.toString(jsonValue);
  }

}

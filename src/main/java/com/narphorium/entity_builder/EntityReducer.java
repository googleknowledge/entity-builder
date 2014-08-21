package com.narphorium.entity_builder;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

public class EntityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  private EntityFrame entityFrame = new EntityFrame();
  private int rank;

  @Override
  public void configure(JobConf conf) {
    rank = conf.getInt("rank", 0);
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
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator,
   * org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void reduce(Text entityId, Iterator<Text> fragments, OutputCollector<Text, Text> entities,
      Reporter reporter) throws IOException {
    Map<String, Object> object = new TreeMap<String, Object>();
    
    // Merge object fragments into a single object
    while (fragments.hasNext()) {
      String triple = fragments.next().toString();
      Map<String, Object> tripleData = (Map<String, Object>) JsonUtils.fromString(triple);
      for (Map.Entry<String, Object> entry2 : tripleData.entrySet()) {
        if (entry2.getKey().equals("@id")) {
          object.put(entry2.getKey(), entry2.getValue());
        } else if (object.containsKey(entry2.getKey())) {
          ((List<Object>) object.get(entry2.getKey())).addAll((List<Object>) entry2.getValue());
        } else {
          object.put(entry2.getKey(), entry2.getValue());
        }
      }
    }

    // Pivot object
    boolean pivoted = false;
    for (String pivotProperty : entityFrame.getPivots(rank)) {
      String reversedProperty = "!" + pivotProperty;
      Map<String, Object> cleanObject = new TreeMap<String, Object>(object);
      cleanObject.remove(reversedProperty);
      if (object.containsKey(reversedProperty)) {
        for (Object pivotNode : (List<Object>) object.get(reversedProperty)) {
          Map<String, Object> pivotedObject = new TreeMap<String, Object>();
          List<Object> objects = new ArrayList<Object>();
          objects.add(cleanObject);
          pivotedObject.put(pivotProperty, objects);
          if (pivotNode instanceof String) {
            entities.collect(new Text((String) pivotNode),
                new Text(JsonUtils.toString(pivotedObject)));
          } else if (pivotNode instanceof Map) {
            String key = (String) ((Map<String, Object>) pivotNode).get("@id");
            entities.collect(new Text(key), new Text(JsonUtils.toString(pivotedObject)));
          }
          pivoted = true;
        }
      }
    }

    if (!pivoted) {
      entities.collect(entityId, new Text(JsonUtils.toString(object)));
    }

  }

}

package com.narphorium.entity_builder;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class EntityFrameReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, MongoUpdateWritable> {

  private EntityFrame entityFrame = new EntityFrame();
  private JsonLdOptions options = new JsonLdOptions();

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

    options.setExplicit(false);
    options.setCompactArrays(true);
    options.setEmbed(true);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator,
   * org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  public void reduce(Text entityId, Iterator<Text> fragments,
      OutputCollector<NullWritable, MongoUpdateWritable> framedEntities, Reporter arg3) throws IOException {
    while (fragments.hasNext()) {
      String triple = fragments.next().toString();
      Map<String, Object> jsonData = (Map<String, Object>) JsonUtils.fromString(triple);
      Map<String, Object> framedEntity;
      try {
        framedEntity = JsonLdProcessor.frame(jsonData, entityFrame.getFrame(), options);
        List<Object> entities = (List<Object>)framedEntity.get("@graph");
        BasicBSONObject bsonQuery = new BasicBSONObject("_id", entityId.toString());
        BasicBSONObject bsonObject = new BasicBSONObject();
        bsonObject.putAll((Map<String, Object>)entities.get(0));
        BasicBSONObject bsonUpdate = new BasicBSONObject();
        bsonUpdate.put("$set", bsonObject);
        framedEntities.collect(null, new MongoUpdateWritable(bsonQuery, bsonUpdate, true, false));
      } catch (JsonLdError e) {
        e.printStackTrace();
      }
    }

  }

}

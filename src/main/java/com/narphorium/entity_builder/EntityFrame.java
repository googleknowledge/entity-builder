package com.narphorium.entity_builder;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class EntityFrame {

  private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

  private Object frame;
  private Set<String> mappedTypes;
  private Set<String> mappedPredicates;
  private Map<Integer, Set<String>> pivotsByRank;
  private int maxRank;

  public void parse(File file) throws IOException {
    parse(new FileInputStream(file));
  }

  public void parse(InputStream inputStream) throws IOException {
    frame = JsonUtils.fromInputStream(inputStream);

    JsonLdOptions options = new JsonLdOptions();
    options.setExplicit(false);
    options.setEmbed(true);
    options.setExpandContext(frame);
    try {
      Object expanded = JsonLdProcessor.expand(frame, options);
      Object flattened = JsonLdProcessor.flatten(frame, options);

      mappedTypes = new TreeSet<String>();
      for (Map<String, Object> flatObj : (List<Map<String, Object>>) flattened) {
        if (flatObj.containsKey("@type")) {
          String type = ((List) flatObj.get("@type")).get(0).toString();
          mappedTypes.add(type);
        }
      }

      mappedPredicates = findMappedPredicates(flattened);
      mappedPredicates.remove("@id");
      mappedPredicates.remove("@type");
      mappedPredicates.add(RDF_TYPE);

      pivotsByRank = new TreeMap<Integer, Set<String>>();
      findPivots(expanded, pivotsByRank);

      for (Map.Entry<Integer, Set<String>> entry : pivotsByRank.entrySet()) {
        for (String pred : entry.getValue()) {
          mappedPredicates.add("!" + pred);
        }
      }
    } catch (JsonLdError e) {
      e.printStackTrace();
    }
  }

  private int findPivots(Object node, Map<Integer, Set<String>> pivotsByRank) {
    if (node instanceof Map) {
      int maxRank = -1;
      if (!((Map<String, Object>) node).isEmpty()) {
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) node).entrySet()) {
          int subRank = findPivots(entry.getValue(), pivotsByRank);
          if (subRank >= 0) {
            Set<String> pivots;
            if (pivotsByRank.containsKey(subRank)) {
              pivots = pivotsByRank.get(subRank);
            } else {
              pivots = new TreeSet<String>();
              pivotsByRank.put(subRank, pivots);
            }
            pivots.add(entry.getKey());
          }
          maxRank = Math.max(maxRank, subRank);
          this.maxRank = Math.max(maxRank, this.maxRank);
        }
        maxRank++;
      }
      return maxRank;
    } else if (node instanceof List) {
      int maxRank = -1;
      for (Object element : (List<Object>) node) {
        int subRank = findPivots(element, pivotsByRank);
        maxRank = Math.max(maxRank, subRank);
        this.maxRank = Math.max(maxRank, this.maxRank);
      }
      return maxRank;
    } else {
      return -1;
    }
  }

  private Set<String> findMappedPredicates(Object node) {
    Set<String> mappedPredicates = new TreeSet<String>();
    if (node instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) node;
      mappedPredicates.addAll(map.keySet());
      for (Object value : map.values()) {
        mappedPredicates.addAll(findMappedPredicates(value));
      }
    } else if (node instanceof List) {
      for (Object element : (List<Object>) node) {
        mappedPredicates.addAll(findMappedPredicates(element));
      }
    }
    return mappedPredicates;
  }

  public Object getFrame() {
    return frame;
  }

  public Set<String> getMappedTypes() {
    return mappedTypes;
  }

  public Set<String> getMappedPredicates() {
    return mappedPredicates;
  }

  public Set<String> getPivots(int rank) {
    return pivotsByRank.containsKey(rank) ? pivotsByRank.get(rank) : new HashSet<String>();
  }

  public int getMaxRank() {
    return maxRank;
  }

}

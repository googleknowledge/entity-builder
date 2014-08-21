package com.narphorium.entity_builder;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NTriplesParser {

  private static Pattern languagePattern = Pattern.compile(".+@([\\w-]*)$");
  private Map<String, String> uriPrefixes = new HashMap<String, String>();

  public void addPrefix(String prefix, String uri) {
    uriPrefixes.put(prefix, uri);
  }

  protected String parseUri(String uri) {
    uri = uri.trim();
    if (uri.length() == 0) {
      return null;
    }
    if (uri.charAt(0) == '<' && uri.charAt(uri.length() - 1) == '>') {
      return uri.substring(1, uri.length() - 1);
    } else {
      String parts[] = uri.split(":");
      if (parts.length == 2 && uriPrefixes.containsKey(parts[0])) {
        return uriPrefixes.get(parts[0]) + parts[1];
      }
    }
    return null;
  }

  public String parsePrefixedUri(String uri) {
    String parts[] = uri.split(":");
    if (parts.length == 2 && uriPrefixes.containsKey(parts[0])) {
      return uriPrefixes.get(parts[0]) + parts[1];
    }
    return uri;
  }

  public String applyPrefix(String uri) {
    for (Map.Entry<String, String> entry : uriPrefixes.entrySet()) {
      if (uri.startsWith(entry.getValue())) {
        return entry.getKey() + ":" + uri.substring(entry.getValue().length());
      }
    }
    return uri;
  }

  public RdfTriple parse(String line) {
    line = line.trim();
    if (!line.isEmpty() && line.charAt(line.length() - 1) == '.') {
      line = line.substring(0, line.length() - 1).trim();
    } else {
      return null;
    }

    if (line.startsWith("@prefix")) {
      line = line.substring(7).trim();
      String parts[] = line.split(":", 2);
      String prefix = parts[0].trim();
      String uri = parts[1].trim();
      uri = uri.substring(1, uri.length() - 1);
      addPrefix(prefix, uri);
    }

    String parts[] = line.split("\\s+");
    if (parts.length > 2) {
      String subjectUri = parseUri(parts[0]);
      String predicateUri = parseUri(parts[1]);
      if (subjectUri != null && predicateUri != null) {
        String object = line.substring(line.indexOf(parts[1]) + parts[1].length());
        String objectUri = parseUri(object);
        if (objectUri != null) {
          return new RdfTriple(subjectUri, predicateUri, objectUri);
        }

        String objectValue = object.trim();
        String objectLanguage = null;
        Matcher m = languagePattern.matcher(object);
        if (m.find()) {
          objectValue = object.substring(0, m.start(1) - 1).trim();
          objectLanguage = m.group(1);
        }
        if (objectValue != null) {
          if (objectValue.startsWith("\"") && objectValue.endsWith("\"")) {
            objectValue = objectValue.substring(1, objectValue.length() - 1);
          }
          return new RdfTriple(subjectUri, predicateUri, objectValue, objectLanguage);
        }
      }
    }
    return null;
  }

  public void clearPrefixes() {
    uriPrefixes.clear();
  }


}

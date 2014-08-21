package com.narphorium.entity_builder;

public class RdfTriple {

  private String subjectUri;
  private String predicateUri;
  private String objectUri;
  private String objectValue;
  private String objectLanguage;

  public RdfTriple() {}

  public RdfTriple(String subjectUri, String predicateUri, String objectUri) {
    this.subjectUri = subjectUri;
    this.predicateUri = predicateUri;
    this.objectUri = objectUri;
  }

  public RdfTriple(String subjectUri, String predicateUri, String objectValue,
      String objectLanguage) {
    this.subjectUri = subjectUri;
    this.predicateUri = predicateUri;
    this.objectValue = objectValue;
    this.objectLanguage = objectLanguage;
  }

  public String getSubject() {
    return subjectUri;
  }

  public String getPredicate() {
    return predicateUri;
  }

  public String getObject() {
    if (objectUri != null) {
      return objectUri;
    } else if (objectValue != null) {
      return objectValue;
    }
    return null;
  }

  public String getLanguage() {
    return objectLanguage;
  }

  public boolean isLiteral() {
    return objectUri == null;
  }
}

package com.zhang2014.project.syntactical;

public enum Delimiters
{
  EQUALS("="), COMMA(","), LEFT_BRACKET("("), RIGHT_BRACKET(")"), ASTERISK("*"), QUESTION("?"), PLUS("+"), MINUS("-"),
  MULTIPLY("*"), DIVIDE("/"), MODULO("%"), DOT("."), ARRAY_LEFT("["), ARRAY_RIGHT("]"), COLON(":");

  private final String value;

  Delimiters(String value)
  {
    this.value = value;
  }

  public String getValue()
  {
    return value;
  }
}

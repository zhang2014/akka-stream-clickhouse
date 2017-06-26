package com.zhang2014.project.syntactical.lookup

import com.zhang2014.project.syntactical.BaseParser
import com.zhang2014.project.syntactical.ast.LookupIndices

trait LookupQueryParser extends BaseParser
{
  lazy val lookupQuery = "lookup" ~ lookupIndices ^^ { case _ ~ x => x }

  lazy val lookupIndices = "indices" ~ tableNameWithDB ~ numericLit ^^
    { case _ ~ nameWithDb ~ partition => LookupIndices(nameWithDb._1, nameWithDb._2, partition.toInt) }

  lexical.reserved +=("lookup", "indices")
}

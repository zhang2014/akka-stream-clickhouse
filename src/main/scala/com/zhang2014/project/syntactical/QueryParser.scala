package com.zhang2014.project.syntactical

import com.zhang2014.project.syntactical.create.CreateQueryParser
import com.zhang2014.project.syntactical.select.SelectQueryParser

object QueryParser extends BaseParser with CreateQueryParser with SelectQueryParser
{
  def query = createQuery | selectQuery
}

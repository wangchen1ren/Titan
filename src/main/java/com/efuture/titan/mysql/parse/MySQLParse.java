/*
 * Copyright 2012-2015 com.efuture.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.efuture.titan.mysql.parse;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public final class MySQLParse {

  public static final int OTHER = 0;
  public static final int BEGIN = 1;
  public static final int COMMIT = 2;
  public static final int DELETE = 3;
  public static final int INSERT = 4;
  public static final int REPLACE = 5;
  public static final int ROLLBACK = 6;
  public static final int SELECT = 7;
  public static final int SET = 8;
  public static final int SHOW = 9;
  public static final int START = 10;
  public static final int UPDATE = 11;
  public static final int KILL = 12;
  public static final int SAVEPOINT = 13;
  public static final int USE = 14;
  public static final int EXPLAIN = 15;
  public static final int KILL_QUERY = 16;
  public static final int HELP = 17;
  public static final int MYSQL_CMD_COMMENT = 18;
  public static final int MYSQL_COMMENT = 19;

  public static final Map<String, Integer> tokenMap = new HashMap<String, Integer>();
  static {
    tokenMap.put("begin", BEGIN);
    tokenMap.put("commit", COMMIT);
    tokenMap.put("delete", DELETE);
    tokenMap.put("insert", INSERT);
    tokenMap.put("replace", REPLACE);
    tokenMap.put("rollback", ROLLBACK);
    tokenMap.put("select", SELECT);
    tokenMap.put("set", SET);
    tokenMap.put("show", SHOW);
    tokenMap.put("start", START);
    tokenMap.put("update", UPDATE);
    tokenMap.put("kill", KILL);
    tokenMap.put("savepoint", SAVEPOINT);
    tokenMap.put("use", USE);
    tokenMap.put("explain", EXPLAIN);
    tokenMap.put("help", HELP);
  }

  public static int parse(String sql) {
    if (sql.length() <= 0) {
      return OTHER;
    }

    if (sql.startsWith("/*!") && sql.endsWith("*/")) {
      //such as /*!40101 SET character_set_client = @saved_cs_client */;
      return MYSQL_CMD_COMMENT;
    }

    sql = ParseUtil.removeComment(sql);
    if (sql.length() <= 0) {
      return MYSQL_COMMENT;
    }

    StringTokenizer st = new StringTokenizer(sql);
    Integer type = tokenMap.get(st.nextToken());
    if (type != null) {
      return type.intValue();
    } else {
      return OTHER;
    }
  }
}

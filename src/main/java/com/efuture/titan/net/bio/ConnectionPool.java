
package com.efuture.titan.net.bio;

import java.sql.Connection;

import com.efuture.titan.session.SessionState;
import com.efuture.titan.metastore.DataNode;

public class ConnectionPool {

  public static Connection newConnection(SessionState ss,
      DataNode node, int replicaIndex) {
    return null;
  }

  public static Connection getConnection(SessionState ss,
      DataNode node, int replicaIndex) {
    return null;
  }

  public static void clear(SessionState ss) {
  }

}

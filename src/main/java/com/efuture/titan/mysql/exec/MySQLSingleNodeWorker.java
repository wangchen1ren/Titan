
package com.efuture.titan.mysql.exec;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.cobar.config.ErrorCode;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.exec.SingleNodeWorker;
import com.efuture.titan.session.SessionState;

public class MySQLSingleNodeWorker extends SingleNodeWorker {
  private static final Log LOG = LogFactory.getLog(MySQLSingleNodeWorker.class);

  public MySQLSingleNodeWorker(TitanConf conf, SessionState ss) {
    super(conf, ss);
  }

  /*============================*/
  /*          Execute           */
  /*============================*/

  @Override
  protected void _execute(Connection connection, String sql) {
    Statement statement = null;
    ResultSet res = null;
    try {
      statement = connection.createStatement();
      res = statement.executeQuery(sql);
      sendResultSet(res);
    } catch (SQLException e) {
      handleError(e);
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (res != null) {
          res.close();
        }
      } catch (SQLException e) {
        LOG.warn("Error in execute sql: " + sql, e);
        // ignore
      }
    }
  }

  private void sendResultSet(ResultSet res) {
  }

  private void handleError(SQLException e) {
  }
 
  /*============================*/
  /*          Commit            */
  /*============================*/

  @Override
  protected void _commit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
    }
  }


  /*============================*/
  /*         Rollback           */
  /*============================*/

  @Override
  protected void _rollback(Connection conn) {
    try {
      conn.rollback();
    } catch (SQLException e) {
    }
  }

}

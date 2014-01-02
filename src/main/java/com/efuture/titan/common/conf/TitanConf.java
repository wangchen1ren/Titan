
package com.efuture.titan.common.conf;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TitanConf extends Configuration {

  public static final Log LOG = LogFactory.getLog(TitanConf.class);
  private static final String DEFAULT_CONF_FILE = "titan-default.xml";
  private static final String USER_CONF_FILE = "titan-site.xml";

  private static final int nProcessors = Runtime.getRuntime().availableProcessors();

  public static enum ConfVars {
    TITAN_MANAGER_PORT("titan.manager.port", (int)9066),

    TITAN_CHARSET("titan.charset", "UTF-8"),

    TITAN_SERVER_PORT("titan.server.port", (int)8066),
    TITAN_SERVER_TIME_UPDATE_INTERVAL("titan.server.time.update.interval", 20L),
    TITAN_SERVER_PROCESSORS("titan.server.processors", nProcessors),
    TITAN_SERVER_PROCESSOR_CHECK_INTERVAL("titan.server.processor.check.interval",
        (long)15 * 1000L),
    TITAN_SERVER_IDLE_TIMEOUT("titan.server.idle.timeout", 30 * 60 * 1000L),

    TITAN_DATANODE_IDLE_CHECK_INTERVAL("titan.datanode.idle.check.interval", 60 * 1000L),
    TITAN_DATANODE_HEARTBEAT_INTERVAL("titan.datanode.heartbeat.interval", 10 * 1000L),

    TITAN_CLUSTER_HEARTBEAT_INTERVAL("titan.cluster.heartbeat.interval", 5 * 1000L),

    /*
    TITAN_CONNECT_POOL_SIZE("titan.connect.pool.size", (int)128),
    TITAN_SYS_WAIT_TIMEOUT("titan.sys.wait.timeout", (long)10 * 1000L),
    TITAN_PARSER_COMMENT_VERSION = 50148;
    // datanode
    TITAN_DATANODE_IDLE_CHECK_PERIOD = 60 * 1000L;
    TITAN_DATANODE_HEARTBEAT_PERIOD = 10 * 1000L;

    // cluster
    TITAN_CLUSTER_HEARTBEAT_PERIOD = 5 * 1000L;
    TITAN_CLUSTER_HEARTBEAT_TIMEOUT = 10 * 1000L;
    TITAN_CLUSTER_HEARTBEAT_RETRY = 10; 
    TITAN_CLUSTER_HEARTBEAT_USER = "_HEARTBEAT_USER_";
    TITAN_CLUSTER_HEARTBEAT_PASS = "_HEARTBEAT_PASS_";

    TITAN_SQL_RECORD_COUNT = 10; 
    TITAN_USE_WR_FLUX_CONTRL = 0;
    */
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1; 
      this.defaultLongVal = -1; 
      this.defaultFloatVal = -1; 
      this.defaultBoolVal = false;
    }   

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = null;
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }

    public String toString() {
      return varname;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    assert (var.valClass == Integer.class);
    conf.setInt(var.varname, val);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public void setIntVar(ConfVars var, int val) {
    setIntVar(this, var, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert (var.valClass == Long.class);
    conf.setLong(var.varname, val);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class);
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    assert (var.valClass == Float.class);
    conf.setFloat(var.varname, val);
  }

  public float getFloatVar(ConfVars var) {
    return getFloatVar(this, var);
  }

  public void setFloatVar(ConfVars var, float val) {
    setFloatVar(this, var, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    conf.setBoolean(var.varname, val);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public void setBoolVar(ConfVars var, boolean val) {
    setBoolVar(this, var, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    assert (var.valClass == String.class);
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public TitanConf() {
    super(false); // don't load hadoop's default configuration file
    URL defaultConfUrl = getClassLoader().getResource(DEFAULT_CONF_FILE);
    if (defaultConfUrl == null) {
      LOG.warn(DEFAULT_CONF_FILE + "not found.");
    } else {
      // load configuration
      addResource(defaultConfUrl);
    }

    URL userConfUrl = getClassLoader().getResource(USER_CONF_FILE);
    if (userConfUrl == null) {
      LOG.debug(USER_CONF_FILE + " not found.");
    } else {
      addResource(USER_CONF_FILE);
    }
  }

}
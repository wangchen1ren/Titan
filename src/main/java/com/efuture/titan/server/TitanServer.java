
package com.efuture.titan.server;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;
import com.efuture.titan.common.TimeUtil;
import com.efuture.titan.net.NIOAcceptor;
import com.efuture.titan.net.NIOConnector;
import com.efuture.titan.net.NIOProcessor;

/*
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;

import com.alibaba.cobar.config.model.SystemConfig;
import com.alibaba.cobar.manager.ManagerConnectionFactory;
import com.alibaba.cobar.mysql.MySQLDataNode;
import com.alibaba.cobar.net.NIOAcceptor;
import com.alibaba.cobar.net.NIOConnector;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.cobar.server.ServerConnectionFactory;
import com.alibaba.cobar.statistic.SQLRecorder;
import com.alibaba.cobar.util.ExecutorUtil;
import com.alibaba.cobar.util.NameableExecutor;
import com.alibaba.cobar.util.TimeUtil;
*/

public class TitanServer {
    public static final Log LOG = LogFactory.getLog(TitanServer.class);
    public static final String SERVER_NAME = "TitanServer";

    private static final TitanServer INSTANCE = new TitanServer();

    private final TitanConf conf;
    private final Timer timer;
    private final NameableExecutor timerExecutor;
    //private final NameableExecutor managerExecutor;
    //private final NameableExecutor initExecutor;
    //private final SQLRecorder sqlRecorder;
    private final AtomicBoolean isOnline;
    private final long startupTime;
    private NIOProcessor[] processors;
    private NIOConnector connector;
    //private NIOAcceptor manager;
    private NIOAcceptor server;

    public static final TitanServer getInstance() {
        return INSTANCE;
    }

    private TitanServer(TitanConf conf) {
        if (conf == null) {
          this.conf = new TitanConf();
        } else {
          this.conf = conf;
        }
        this.timer = new Timer(SERVER_NAME + "Timer", true);
        this.timerExecutor = NameableExecutor.newExecutor("TimerExecutor",
            conf.getIntVar(ConfVars.TITAN_TIMER_EXECUTOR));

        /*
        this.initExecutor = ExecutorUtil.create("InitExecutor", system.getInitExecutor());
        this.managerExecutor = ExecutorUtil.create("ManagerExecutor", system.getManagerExecutor());
        this.sqlRecorder = new SQLRecorder(system.getSqlRecordCount());
        */
        this.isOnline = new AtomicBoolean(true);
        this.startupTime = TimeUtil.currentTimeMillis();
    }

    public TitanConf getConf() {
        return conf;
    }

    public void startup() throws IOException {
        // server startup
        LOG.info("===============================================");
        LOG.info(SERVER_NAME + " is ready to startup ...");
        timer.schedule(updateTime(), 0L,
            conf.getLongVar(ConfVars.TIME_UPDATE_INTERVAL));

        // startup processors
        LOG.info("Startup Processors ...");
        int nProcessor = conf.getIntVar(ConfVars.TITAN_SERVER_PROCESSORS);
        int nExecutor = conf.getIntVar(ConfVars.TITAN_SERVER_EXECUTORS);
        LOG.info("Processors: " + nProcessor + ", Executors: " + nExecutor);
        processors = new NIOProcessor[nProcessor];
        for (int i = 0; i < processors.length; i++) {
            //processors[i] = new NIOProcessor("Processor" + i, handler, executor);
            processors[i] = new NIOProcessor("Processor" + i, 0, nExecutor);
            processors[i].startup();
        }
        timer.schedule(checkProcessor(), 0L,
            conf.getLongVar(ConfVars.TITAN_SERVER_PROCESSOR_CHECK_INTERVAL));

        // startup connector
        LOG.info("Startup Connector ...");
        connector = new NIOConnector(SERVER_NAME + "Connector");
        connector.setProcessors(processors);
        connector.start();

        // init datanodes
        LOG.info("Initialize DataNodes ...");
        List<DataNodeConf> dataNodeConfs = conf.getDataNodeConfs();
        List<DataNode> dataNodes = new ArrayList<DataNode>();
        for (DataNodeConf dnConf : dataNodeConfs) {
          DataNode node = DataNodeBuilder.setDataNodeConf(dnConf).build();
          node.init(0);
          node.startHeartbeat();
        }
        timer.schedule(checkDataNodeIdle(), 0L,
            conf.getLongVar(ConfVars.TITAN_DATANODE_IDLE_CHECK_INTERVAL));
        timer.schedule(heartbeatDataNode(), 0L,
            conf.getLongVar(ConfVars.TITAN_DATANODE_HEARTBEAT_INTERVAL));

        /*
        // startup manager
        ManagerConnectionFactory mf = new ManagerConnectionFactory();
        mf.setCharset(system.getCharset());
        mf.setIdleTimeout(system.getIdleTimeout());
        manager = new NIOAcceptor(NAME + "Manager", system.getManagerPort(), mf);
        manager.setProcessors(processors);
        manager.start();
        LOG.info(manager.getName() + " is started and listening on " + manager.getPort());
        */

        // startup server
        ServerConnectionFactory sf = new ServerConnectionFactory();
        sf.setCharset(conf.getVar(ConfVars.TITAN_CHARSET));
        sf.setIdleTimeout(conf.getIntVar(ConfVars.TITAN_SERVER_IDLE_TIMEOUT));
        int port = conf.getIntVar(ConfVars.TITAN_SERVER_PORT);
        server = new NIOAcceptor(SERVER_NAME, port, sf);
        server.setProcessors(processors);
        LOG.info("Starting " + server.getName() + " ...");
        server.start();
        timer.schedule(heartbeatCluster(), 0L,
            conf.getLongVar(ConfVars.TITAN_CLUSTER_HEARTBEAT_INTERVAL));
        LOG.info(server.getName() + " has been started and listening on port " + server.getPort());
        LOG.info("===============================================");
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public NIOConnector getConnector() {
        return connector;
    }

    public NameableExecutor getManagerExecutor() {
        return managerExecutor;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }

    public NameableExecutor getInitExecutor() {
        return initExecutor;
    }

    public SQLRecorder getSqlRecorder() {
        return sqlRecorder;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    // 系统时间定时更新任务
    private TimerTask updateTime() {
        return new TimerTask() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // 处理器定时检查任务
    private TimerTask checkProcessor() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (NIOProcessor p : processors) {
                            p.check();
                        }
                    }
                });
            }
        };
    }

    // 数据节点定时连接空闲超时检查任务
    private TimerTask checkDataNodeIdle() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, MySQLDataNode> nodes = config.getDataNodes();
                        for (MySQLDataNode node : nodes.values()) {
                            node.idleCheck();
                        }
                        Map<String, MySQLDataNode> _nodes = config.getBackupDataNodes();
                        if (_nodes != null) {
                            for (MySQLDataNode node : _nodes.values()) {
                                node.idleCheck();
                            }
                        }
                    }
                });
            }
        };
    }

    // 数据节点定时心跳任务
    private TimerTask heartbeatDataNode() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, MySQLDataNode> nodes = config.getDataNodes();
                        for (MySQLDataNode node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }

    // 集群节点定时心跳任务
    private TimerTask clusterHeartbeat() {
        return new TimerTask() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, CobarNode> nodes = config.getCluster().getNodes();
                        for (CobarNode node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }


    public static void main(String[] args) {
      try {
        LogUtil.initLog4j(SERVER_NAME);
        TitanConf conf = new TitanConf();
        TitanServer server = new TitanServer(conf);
        server.startup();
      } catch (Exception e) {
        Log.error("TitanServer startup error: "
            + StringUtils.stringifyException(e));
        System.exit(1);
      }
    }
}

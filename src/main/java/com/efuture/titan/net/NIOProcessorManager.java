
package com.efuture.titan.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.common.conf.TitanConf.ConfVars;

public class NIOProcessorManager {
  private static final Log LOG = LogFactory.getLog(NIOProcessorManager.class);

  private List<NIOProcessor> processors;
  private int nProcessor;
  private int idx;

  public NIOProcessorManager(TitanConf conf, String name) throws IOException {
    nProcessor = conf.getIntVar(ConfVars.TITAN_SERVER_PROCESSORS);
    LOG.info("Processors: " + nProcessor);
    processors = new ArrayList<NIOProcessor>();
    for (int i = 0; i < nProcessor; ++i) {
      processors.add(new NIOProcessor(conf, name + "-" + i));
    }
    idx = 0;
  }

  public void start() {
    for (NIOProcessor processor : processors) {
      processor.start();
    }
  }

  public NIOProcessor nextProcessor() {
    NIOProcessor res = processors.get(idx);
    idx = (idx + 1) % nProcessor;
    return res;
  }
}

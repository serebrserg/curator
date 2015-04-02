/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.leader;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class ManualControlCnxnFactory extends NIOServerCnxnFactory {

    public static final int LOCKOUT_DURATION_MS = 3000;
    public static final int SESSION_TIMEOUT = 30000;
    public static final String LOCK_PATH = "testcnxn";
    public static final String KILL_CONNECTION_PATH = "killConnection";

    public ManualControlCnxnFactory() throws IOException {
    }

    private static final Logger log = LoggerFactory.getLogger(ManualControlCnxnFactory.class);

    @Override
    public void startup(ZooKeeperServer zks) throws IOException, InterruptedException
    {
        super.startup(new ManualCnxnControlZookeekeprServer(zks));
    }

    /**
     * Build a connection with a Chaos Monkey ZookeeperServer
     */
    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk) throws IOException
    {
        return new NIOServerCnxn(zkServer, sock, sk, this);
    }

    public static class ManualCnxnControlZookeekeprServer extends ZooKeeperServer
    {

        private List<NIOServerCnxn> myConnections = new ArrayList<NIOServerCnxn>();
        long firstError = 0;
        long sessionId = 0;


        public ManualCnxnControlZookeekeprServer(ZooKeeperServer zks)
        {
            setTxnLogFactory(zks.getTxnLogFactory());
            setTickTime(zks.getTickTime());
            setMinSessionTimeout(SESSION_TIMEOUT);
            setMaxSessionTimeout(SESSION_TIMEOUT);
        }


        @Override
        public void submitRequest(Request si)
        {
            log.debug("Applied : " + si.toString());
            if (si.toString().contains(KILL_CONNECTION_PATH)){
                log.debug("closing connection:"+myConnections.get(0));
                myConnections.get(0).close();
                firstError = System.currentTimeMillis();
            }
            if (si.toString().contains(LOCK_PATH) && si.type == ZooDefs.OpCode.create){
                myConnections.add((NIOServerCnxn) si.cnxn);
                sessionId = si.sessionId;
                log.debug("adding connection : " + si.cnxn);
            }

            if (si.sessionId == sessionId && System.currentTimeMillis() < firstError + LOCKOUT_DURATION_MS){
                log.debug("rejecting");
                ((NIOServerCnxn)si.cnxn).close();
                return;
            }
            super.submitRequest(si);
            log.debug("Connections : " + myConnections);

        }

    }
}

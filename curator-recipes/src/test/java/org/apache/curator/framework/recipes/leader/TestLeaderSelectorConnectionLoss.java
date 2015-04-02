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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class TestLeaderSelectorConnectionLoss {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeClass
    public static void setCNXFactory()
    {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, ManualControlCnxnFactory.class.getName());
    }

    @AfterClass
    public static void resetCNXFactory()
    {
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
    }


    /**
     * Create a LeaderSelector but close the connection after the "lock" znode
     * has been created.
     * Then re-establish the connection using the same session and find not deleted ephemeral node.
     *
     * @throws Exception
     */
    @Test
    public void connectionLossSameSessionTest() throws Exception
    {
        Timing timing = new Timing();
        int port = 2181;

        String lockPath = "/" + ManualControlCnxnFactory.LOCK_PATH;

        String killConnectionPath = "/" + ManualControlCnxnFactory.KILL_CONNECTION_PATH;
        int waitTillReconnected = 7000;

        TestingServer server = new TestingServer(port);

        final CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .retryPolicy(new RetryNTimes(1, 500))
                        .sessionTimeoutMs(ManualControlCnxnFactory.SESSION_TIMEOUT)
                        .build();

        final CuratorFramework watcherClient =
                CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .retryPolicy(new RetryNTimes(1, 500))
                        .sessionTimeoutMs(ManualControlCnxnFactory.SESSION_TIMEOUT)
                        .build();

        final TestLeaderSelectorListener listener = new TestLeaderSelectorListener();
        LeaderSelector leaderSelector1 =
                new LeaderSelector(client, lockPath, listener);

        client.start();
        client.create().creatingParentsIfNeeded().forPath(lockPath);

        watcherClient.start();

        try
        {
            leaderSelector1.autoRequeue();
            leaderSelector1.start();
            timing.sleepABit();
            List<String> children = watcherClient.getChildren().forPath(lockPath);
            log.debug("children initial : {}", children);

            watcherClient.create().creatingParentsIfNeeded().forPath(killConnectionPath);

            Thread.sleep(waitTillReconnected);
            List<String> children2 = watcherClient.getChildren().forPath(lockPath);
            log.debug("children after reconnect : {}", children2);
            Assert.assertEquals(children2.size(), 1, "Number of lock nodes after reconnect must be 1");
            Assert.assertFalse(children2.contains(children.get(0)),
                    "Nodes after reconnect must not contain the node created before connection was lost");
        }
        finally
        {
            try
            {
                leaderSelector1.close();
            }
            catch ( IllegalStateException e )
            {
                Assert.fail(e.getMessage());
            }
            client.close();
        }
    }

    private class TestLeaderSelectorListener extends LeaderSelectorListenerAdapter {


        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
            try {
                log.info("-->takeLeadership({})", client.toString());

                Thread.sleep(100000);
                log.info("<--takeLeadership({})", client.toString());
            }catch(InterruptedException e){
                log.info("interrupted");
                throw e;
            }
        }



    }
}

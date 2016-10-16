package org.yujy.service.es;


import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * 连接es集群的客户端.
 */
public class TransportClientUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientUtils.class);

    public void create() {

        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", "myClusterName").build();

            Client client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException ex) {
            LOGGER.error("无法解析连接地址");
        }

    }
}

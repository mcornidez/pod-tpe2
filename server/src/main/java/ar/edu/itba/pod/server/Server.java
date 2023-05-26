package ar.edu.itba.pod.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args){
        logger.info("tpe2-g2 Server Starting ...");

        //Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("tpe2-g2").setPassword("tpe2-g2-pass");
        config.setGroupConfig(groupConfig);

        // Network Config
        MulticastConfig multicastConfig = new MulticastConfig();

        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(List.of("192.168.1.*"))
                .setEnabled(false);

        NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        Hazelcast.newHazelcastInstance(config);
        logger.info("Server started");

    }}

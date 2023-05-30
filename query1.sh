#!/bin/bash

tar -xf "client/target/tpe2-g2-client-2023.1Q-bin.tar.gz" -C "client/target/"
java "$@" -Dhazelcast.logging.type=none  -cp 'client/target/tpe2-g2-client-2023.1Q/lib/jars/*' "ar.edu.itba.pod.client.Query1"
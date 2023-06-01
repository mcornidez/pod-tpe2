#!/bin/bash

tar -xf "server/target/tpe2-l61432-server-2023.1Q-bin.tar.gz" -C "server/target/"
java -cp 'server/target/tpe2-l61432-server-2023.1Q/lib/jars/*' -Xmx8192m "ar.edu.itba.pod.server.Server" "$@"
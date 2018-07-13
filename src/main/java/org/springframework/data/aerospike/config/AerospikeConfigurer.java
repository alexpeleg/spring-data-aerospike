package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

import java.util.Collection;

public interface AerospikeConfigurer {

    Collection<Host> getHosts();

    String nameSpace();

    ClientPolicy getClientPolicy();

    AerospikeClient aerospikeClient();
}

package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;

/**
 * Base class for Spring Data Aerospike configuration using Java configuration.
 */
@Configuration
public abstract class AbstractAerospikeDataConfiguration
        extends AbstractAerospikeTemplateConfiguration implements AerospikeConfigurer {

    @Override
    protected AerospikeConfigurer aerospikeConfigurer() {
        return this;
    }

    @Override
    public abstract Collection<Host> getHosts();

    @Override
    public abstract String nameSpace();

    @Override
    public ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.timeout = 10_000;
        return clientPolicy;
    }

    @Bean(name = BeanNames.AEROSPIKE_CLIENT, destroyMethod = "close")
    @Override
    public AerospikeClient aerospikeClient() {
        Collection<Host> hosts = getHosts();
        return new AerospikeClient(getClientPolicy(), hosts.toArray(new Host[hosts.size()]));
    }
}

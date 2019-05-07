package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.CustomConversions;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.DefaultAerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.*;

@Configuration
public abstract class AbstractAerospikeDataConfiguration {

    @Bean(name = "aerospikeTemplate")
    public AerospikeTemplate aerospikeTemplate(AerospikeClient aerospikeClient,
                                               MappingAerospikeConverter mappingAerospikeConverter,
                                               AerospikeMappingContext aerospikeMappingContext,
                                               AerospikeExceptionTranslator aerospikeExceptionTranslator) {
        return new AerospikeTemplate(aerospikeClient, nameSpace(), mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator);
    }

    @Bean(name = "reactiveAerospikeTemplate")
    public ReactiveAerospikeTemplate reactiveAerospikeTemplate(AerospikeClient aerospikeClient,
                                                               MappingAerospikeConverter mappingAerospikeConverter,
                                                               AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                                               AerospikeReactorClient aerospikeReactorClient) {
        return new ReactiveAerospikeTemplate(aerospikeClient, nameSpace(), mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator, aerospikeReactorClient);
    }

    @Bean(name = "mappingAerospikeConverter")
    public MappingAerospikeConverter mappingAerospikeConverter(AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor) {
        return new MappingAerospikeConverter(aerospikeMappingContext, customConversions(), aerospikeTypeAliasAccessor);
    }

    @Bean(name = "aerospikeTypeAliasAccessor")
    public AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor() {
        return new AerospikeTypeAliasAccessor();
    }

    @Bean(name = "aerospikeCustomConversions")
    public CustomConversions customConversions() {
        return new CustomConversions(customConverters(), simpleTypeHolder());
    }

    protected List<?> customConverters() {
        return Collections.emptyList();
    }

    @Bean(name = "aerospikeMappingContext")
    public AerospikeMappingContext aerospikeMappingContext() throws Exception {
        AerospikeMappingContext context = new AerospikeMappingContext();
        context.setInitialEntitySet(getInitialEntitySet());
        context.setSimpleTypeHolder(simpleTypeHolder());
        context.setFieldNamingStrategy(fieldNamingStrategy());
        context.setDefaultNameSpace(nameSpace());
        return context;
    }

    @Bean(name = "aerospikeExceptionTranslator")
    public AerospikeExceptionTranslator aerospikeExceptionTranslator() {
        return new DefaultAerospikeExceptionTranslator();
    }

    @Bean(name = "aerospikeSimpleTypeHolder")
    protected SimpleTypeHolder simpleTypeHolder() {
        return AerospikeSimpleTypes.HOLDER;
    }

    @Bean(name = "aerospikeClient", destroyMethod = "close")
    public AerospikeClient aerospikeClient() {
        Collection<Host> hosts = getHosts();
        return new AerospikeClient(getClientPolicy(), hosts.toArray(new Host[hosts.size()]));
    }

    @Bean(name = "aerospikeReactorClient")
    public AerospikeReactorClient aerospikeReactorClient(AerospikeClient aerospikeClient, EventLoops eventLoops) {
        return new AerospikeReactorClient(aerospikeClient, eventLoops);
    }

    protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
        String basePackage = getMappingBasePackage();
        Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

        if (StringUtils.hasText(basePackage)) {
            ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
            for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
                initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), AbstractAerospikeDataConfiguration.class.getClassLoader()));
            }
        }

        return initialEntitySet;
    }

    protected String getMappingBasePackage() {
        return getClass().getPackage().getName();
    }

    protected FieldNamingStrategy fieldNamingStrategy() {
        return PropertyNameFieldNamingStrategy.INSTANCE;
    }

    protected abstract Collection<Host> getHosts();

    protected abstract String nameSpace();

    @Bean
    protected abstract EventLoops eventLoops();

    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.timeout = 10_000;
        clientPolicy.eventLoops = eventLoops();
        return clientPolicy;
    }


}

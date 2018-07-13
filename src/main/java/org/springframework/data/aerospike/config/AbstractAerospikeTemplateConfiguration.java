package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
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
public abstract class AbstractAerospikeTemplateConfiguration {

    protected abstract AerospikeConfigurer aerospikeConfigurer();

    @Bean(name = BeanNames.AEROSPIKE_TEMPLATE)
    public AerospikeTemplate aerospikeTemplate(AerospikeClient aerospikeClient,
                                               MappingAerospikeConverter mappingAerospikeConverter,
                                               AerospikeMappingContext aerospikeMappingContext,
                                               AerospikeExceptionTranslator aerospikeExceptionTranslator) {
        return new AerospikeTemplate(aerospikeClient, aerospikeConfigurer().nameSpace(), mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator);
    }

    @Bean(name = BeanNames.MAPPING_AEROSPIKE_CONVERTER)
    public MappingAerospikeConverter mappingAerospikeConverter(AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor) {
        return new MappingAerospikeConverter(aerospikeMappingContext, customConversions(), aerospikeTypeAliasAccessor);
    }

    @Bean(name = BeanNames.AEROSPIKE_TYPE_ALIAS_ACCESSOR)
    public AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor() {
        return new AerospikeTypeAliasAccessor();
    }

    @Bean(name = BeanNames.AEROSPIKE_CUSTOM_CONVERSIONS)
    public CustomConversions customConversions() {
        return new CustomConversions(customConverters(), simpleTypeHolder());
    }

    protected List<?> customConverters() {
        return Collections.emptyList();
    }

    @Bean(name = BeanNames.AEROSPIKE_MAPPING_CONTEXT)
    public AerospikeMappingContext aerospikeMappingContext() throws Exception {
        AerospikeMappingContext context = new AerospikeMappingContext();
        context.setInitialEntitySet(getInitialEntitySet());
        context.setSimpleTypeHolder(simpleTypeHolder());
        context.setFieldNamingStrategy(fieldNamingStrategy());
        context.setDefaultNameSpace(aerospikeConfigurer().nameSpace());
        return context;
    }

    @Bean(name = BeanNames.AEROSPIKE_EXCEPTION_TRANSLATOR)
    public AerospikeExceptionTranslator aerospikeExceptionTranslator() {
        return new DefaultAerospikeExceptionTranslator();
    }

    @Bean(name = BeanNames.AEROSPIKE_SIMPLE_TYPE_HOLDER)
    protected SimpleTypeHolder simpleTypeHolder() {
        return AerospikeSimpleTypes.HOLDER;
    }

    protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
        String basePackage = getMappingBasePackage();
        Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

        if (StringUtils.hasText(basePackage)) {
            ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
            for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
                initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), AbstractAerospikeTemplateConfiguration.class.getClassLoader()));
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

}

package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Base class for implementation tests for {@link AerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public abstract class BaseReactiveAerospikeTemplateTests extends BaseIntegrationTests {
    static final String SET_NAME_PERSON = "Person";
    static final String SET_NAME_VERSIONED = "versioned-set";
    static final String SET_NAME_EXPIRATION = "expiration-set";

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;

    private DefaultRepositoryMetadata repositoryMetaData =  new DefaultRepositoryMetadata(ContactRepository.class);
    WritePolicy defaultWritePolicy = getWritePolicy();

    @Before
    public void cleanUp() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_PERSON,
                (key, record) -> client.delete(null, key));
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_VERSIONED,
                (key, record) -> client.delete(null, key));
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_EXPIRATION,
                (key, record) -> client.delete(null, key));
    }

    void assertPerson(Person person) {
        Record result = client.get(null, new Key(getNameSpace(), SET_NAME_PERSON, person.getId()));
        assertEquals(person.getFirstname(), result.getString("firstname"));
        assertEquals(person.getLastname(), result.getString("lastname"));
    }

    WritePolicy getWritePolicy() {
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;
        return policy;
    }

    <T> Query createQueryForMethodWithArgs(String methodName, Object... args) {
        Class[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
        Method method = ReflectionUtils.findMethod(PersonRepository.class, methodName, argTypes);
        PartTree partTree = new PartTree(method.getName(), Person.class);
        AerospikeQueryCreator creator = new AerospikeQueryCreator(partTree, new ParametersParameterAccessor(new QueryMethod(method, repositoryMetaData, new SpelAwareProxyProjectionFactory()).getParameters(), args));
        return creator.createQuery();
    }

}

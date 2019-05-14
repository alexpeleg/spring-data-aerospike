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
import org.springframework.data.aerospike.sample.Person;

import static org.junit.Assert.assertEquals;

/**
 * Base class for implementation tests for {@link AerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public abstract class BaseReactiveAerospikeTemplateTests extends BaseIntegrationTests {
    private static final String SET_NAME_PERSON = "Person";
    static final String SET_NAME_VERSIONED = "versioned-set";

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;

    @Before
    public void cleanUp() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_PERSON,
                (key, record) -> client.delete(null, key));
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_VERSIONED,
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
}

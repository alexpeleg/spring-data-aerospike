package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.sample.Person;

import java.util.Arrays;

import static com.aerospike.client.policy.RecordExistsAction.UPDATE_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link AerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateIntegrationTests extends BaseIntegrationTests {
    private static final String SET_NAME_PERSON = "Person";
    private static final String SET_NAME_VERSIONED = "versioned-set";

    @Autowired
    private ReactiveAerospikeTemplate reactiveTemplate;

    @Before
    public void cleanUp() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false;
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_PERSON,
                (key, record) -> client.delete(null, key));
        client.scanAll(scanPolicy, getNameSpace(), SET_NAME_VERSIONED,
                (key, record) -> client.delete(null, key));
    }

    @Test
    public void testSave() {
        Person customer = new Person("dave-002", "Dave", "Matthews");
        reactiveTemplate.save(customer).block();
        assertPerson(customer);
    }

    @Test
    public void testSaveVersioned() {
        VersionedClass document = new VersionedClass("foo-1", "foo");
        reactiveTemplate.save(document).block();
        assertThat(document.version).isEqualTo(1);
        reactiveTemplate.save(document).block();
        assertThat(document.version).isEqualTo(2);
        Record result = client.get(null, new Key(getNameSpace(), SET_NAME_VERSIONED, document.getId()));
        assertEquals(document.getField(), result.getString("field"));
    }

    @Test(expected = AerospikeException.class)
    public void testRejectSaveWithOldVersion() {
        VersionedClass document = new VersionedClass("foo-1", "foo");
        reactiveTemplate.save(document).block();
        document.setVersion(0);
        reactiveTemplate.save(document).block();
    }

    @Test
    public void testPersist() {
        Person customer = new Person("dave-002", "Dave", "Matthews");
        WritePolicy writePolicy = getWritePolicy();
        reactiveTemplate.persist(customer, writePolicy).block();
        assertPerson(customer);
    }

    @Test(expected = AerospikeException.class)
    public void testPersistWithUpdateOnlyWritePolicy() {
        Person customer = new Person("dave-002", "Dave", "Matthews");
        WritePolicy writePolicy = getWritePolicy();
        writePolicy.recordExistsAction = UPDATE_ONLY;
        reactiveTemplate.persist(customer, writePolicy).block();
    }

    @Test
    public void testInsertAll() {
        Person customer1 = new Person("dave-002", "Dave", "Matthews");
        Person customer2 = new Person("james-007", "James", "Bond");
        reactiveTemplate.insertAll(Arrays.asList(customer1, customer2)).blockLast();
        assertPerson(customer1);
        assertPerson(customer2);
    }

    @Test
    public void testInsert() {
        Person customer = new Person("dave-002", "Dave", "Matthews");
        reactiveTemplate.insert(customer).block();
        assertPerson(customer);
    }

    @Test
    public void testUpdate() {
        Person customer = new Person("dave-001", "Dave", "Matthews");
        reactiveTemplate.insert(customer).block();
        customer.setLastname(customer.getLastname() + "xx");
        reactiveTemplate.update(customer).block();
        assertPerson(customer);
    }


    private void assertPerson(Person person) {
        Record result = client.get(null, new Key(getNameSpace(), SET_NAME_PERSON, person.getId()));
        assertEquals(person.getFirstname(), result.getString("firstname"));
        assertEquals(person.getLastname(), result.getString("lastname"));
    }

    private WritePolicy getWritePolicy() {
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;
        return policy;
    }


}

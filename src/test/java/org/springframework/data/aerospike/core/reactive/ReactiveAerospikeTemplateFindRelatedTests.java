package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.IndexType;
import org.junit.Test;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;

/**
 * Tests for find related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateFindRelatedTests extends BaseReactiveAerospikeTemplateTests {

    @Test
    public void testFindById() {
        client.put(defaultWritePolicy,
                new Key(getNameSpace(), SET_NAME_PERSON, "dave-003"),
                new Bin("firstname", "Dave"),
                new Bin("lastname", "Matthews"));
        Optional<Person> result = reactiveTemplate.findById("dave-003", Person.class).block();
        assertTrue(result.isPresent());
        assertEquals("Matthews", result.get().getLastname());
        assertEquals("Dave", result.get().getFirstname());
    }

    @Test
    public void testFindByIdNonexistentValue() {
        Optional<Person> result = reactiveTemplate.findById("dave-is-absent", Person.class).block();
        assertFalse(result.isPresent());
        Optional<SampleClasses.DocumentWithTouchOnRead> result2 = reactiveTemplate.findById("foo-is-absent", SampleClasses.DocumentWithTouchOnRead.class).block();
        assertFalse(result2.isPresent());
    }

    @Test
    public void testFindByIdShouldIncreaseVersionIfTouchOnReadSetToTrue() {
        SampleClasses.DocumentWithTouchOnRead document = new SampleClasses.DocumentWithTouchOnRead("foo-1", 1);
        reactiveTemplate.save(document).block();

        Optional<SampleClasses.DocumentWithTouchOnRead> result = reactiveTemplate.findById(document.getId(), SampleClasses.DocumentWithTouchOnRead.class).block();
        assertThat(result.get().getVersion()).isEqualTo(document.getVersion() + 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testFindByIdShouldFailOnTouchOnReadWithExpirationProperty() {
        SampleClasses.DocumentWithTouchOnReadAndExpirationProperty document = new SampleClasses.DocumentWithTouchOnReadAndExpirationProperty("foo-1", EXPIRATION_ONE_MINUTE);
        reactiveTemplate.insert(document).block();
        reactiveTemplate.findById(document.getId(), SampleClasses.DocumentWithTouchOnReadAndExpirationProperty.class);
    }

    @Test
    public void testFindAll() {
        IntStream.rangeClosed(1, 10).forEach(idx ->
                client.put(defaultWritePolicy,
                        new Key(getNameSpace(), SET_NAME_PERSON, "user" + idx),
                        new Bin("firstname", "Dave"),
                        new Bin("lastname", "Matthews"))
        );
        Long userCount = reactiveTemplate.findAll(Person.class).count().block();
        assertThat(userCount).isEqualTo(10);
    }

    @Test
    public void testFindByIds() {
        List<String> ids = new ArrayList<>(10);
        IntStream.rangeClosed(1, 10).forEach(idx -> {
            String id = "user" + idx;
            ids.add(id);
            client.put(defaultWritePolicy,
                    new Key(getNameSpace(), SET_NAME_PERSON, id),
                    new Bin("firstname", "Dave"),
                    new Bin("lastname", "Matthews"));
        });
        Long userCount = reactiveTemplate.findByIds(ids, Person.class).count().block();
        assertThat(userCount).isEqualTo(10);
    }

    @Test
    public void testFindByIdsShouldReturnEmptyList() {
        Long userCount = reactiveTemplate.findByIds(Collections.emptyList(), Person.class).count().block();
        ;
        assertThat(userCount).isEqualTo(0);
    }

    @Test
    public void testFindByIdsShouldFindExisting() {
        Person customer1 = new Person("dave-002", "Dave", "Matthews");
        Person customer2 = new Person("james-007", "James", "Bond");
        Person customer3 = new Person("matt-001", "Matt", "Groening");
        reactiveTemplate.insertAll(Arrays.asList(customer1, customer2, customer3)).blockLast();

        List<String> ids = Arrays.asList("unknown", customer1.getId(), customer2.getId());
        List<Person> actual = reactiveTemplate.findByIds(ids, Person.class).collectList().block();

        assertThat(actual).containsExactlyInAnyOrder(customer1, customer2);
    }

    @Test
    public void testFindInRangeShouldFindLimitedNumberOfDocuments() throws Exception {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person("idx" + id, "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class).collectList().block();
        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void testFindInRangeShouldFindLimitedNumberOfDocumentsAndSkip() throws Exception {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person("idx" + id, "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class).collectList().block();

        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void testFindThrowsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class).collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }


    @Test
    public void testFindWithFilterEqual() {
        createIndexIfNotExists(Person.class, "first_name_index", "firstname", IndexType.STRING);
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person("dave" + id, "Dave", "Matthews")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = createQueryForMethodWithArgs("findPersonByFirstname", "Dave");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyInAnyOrderElementsOf(allUsers);
    }

    @Test
    public void testFindWithFilterEqualOrderBy() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person("dave" + id, "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort(Comparator.comparing(Person::getFirstname)); // Order user list by firstname ascending

        Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameAsc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void testFindWithFilterEqualOrderByDesc() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        createIndexIfNotExists(Person.class, "last_name_index", "lastname", IndexType.STRING);

        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person("dave" + id, "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort((o1, o2) -> o2.getFirstname().compareTo(o1.getFirstname())); // Order user list by firstname descending

        Query query = createQueryForMethodWithArgs("findByLastnameOrderByFirstnameDesc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void testFindWithFilterRange() {
        createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);

        List<Person> allUsers = IntStream.rangeClosed(21, 30)
                .mapToObj(age -> new Person("dave" + age, "Dave" + age, "Matthews", age)).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        List<Person> actual = reactiveTemplate.find(query, Person.class).collectList().block();

        assertThat(actual)
                .hasSize(6)
                .containsExactlyInAnyOrderElementsOf(allUsers.subList(4, 10));
    }

}

package org.springframework.data.aerospike;

import lombok.*;
import org.joda.time.DateTime;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.annotation.Expiration;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.SimpleClassWithPersistenceConstructor.SIMPLESET2;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;

public class SampleClasses {

	public static final int EXPIRATION_ONE_SECOND = 1;
	public static final int EXPIRATION_ONE_MINUTE = 60;

	static interface SomeInterface {
	}

	public static enum TYPES {
		FIRST(1), SECOND(2), THIRD(3);
		final int id;

		TYPES(int id) {
			this.id = id;
		}
	}

	@TypeAlias("simpleclass")
	@Document(collection = SIMPLESET)
	@Data
	public static class SimpleClass implements SomeInterface {

		public static final String SIMPLESET = "simpleset1";
		@Id
		final long id;
		final String field1;
		final int field2;
		final long field3;
		final float field4;
		final double field5;
		final boolean field6;
		final Date field7;
		final TYPES field8;
		final Set<String> field9;
		final Set<Set<String>> field10;
//		TODO: see https://github.com/aerospike/aerospike-client-java/issues/75
//		final byte field10;
//		final char field11;

	}

	@Document
	@Data
	public static class MapWithSimpleValue {
		@Id
		final long id;
		final Map<String, String> mapWithSimpleValue;
	}

	@Document
	@Data
	public static class MapWithCollectionValue {
		@Id
		final long id;
		final Map<String, List<String>> mapWithCollectionValue;
	}

	@Document
	@Data
	public static class MapWithNonSimpleValue {
		@Id
		final long id;
		final Map<String, Address> mapWithNonSimpleValue;
	}

	@Document
	@Data
	public static class SortedMapWithSimpleValue {
		final SortedMap<String, String> map;
	}

	@Document
	@Data
	public static class NestedMapsWithSimpleValue {
		final Map<String, Map<String, Map<String, String>>> nestedMaps;
	}

	@Document
	@Data
	public static class GenericType<T> {
		final T content;
	}

	@Document
	@Data
	public static class ListOfLists {
		final List<List<String>> listOfLists;
	}

	@Document
	@Data
	public static class ListOfMaps {
		final List<Map<String, Name>> listOfMaps;
	}

	@Document
	@Data
	public static class ContainerOfCustomFieldNames {
		@Field("property")
		final String myField;
		final CustomFieldNames customFieldNames;
	}

	@Document
	@Data
	public static class CustomFieldNames {
		@Field("property1")
		final int intField;
		@Field("property2")
		final String stringField;
	}

	@Document
	@Data
	public static class ClassWithComplexId {

		@Id
		final ComplexId complexId;
	}

	@Data
	public static class ComplexId {
		final Long innerId;
	}

	@Document
	@Data
	public static class SetWithSimpleValue {
		@Id
		final long id;
		final Set<String> collectionWithSimpleValues;
	}

	@Document(collection = SIMPLESET2)
	@Data
	public static class SimpleClassWithPersistenceConstructor {

		public static final String SIMPLESET2 = "simpleset2";
		@Id
		final long id;
		final String field1;
		final int field2;

		@PersistenceConstructor
		public SimpleClassWithPersistenceConstructor(long id, String field1, int field2) {
			this.id = id;
			this.field1 = field1;
			this.field2 = field2;
		}
	}

	@Document(collection = SIMPLESET3)
	@Data
	public static class User {
		public static final String SIMPLESET3 = "simpleset3";
		@Id
		final long id;
		final Name name;
		final Address address;
	}

	@Data
	public static class Name {
		final String firstName;
		final String lastName;
	}

	@Data
	public static class Address {
		final Street street;
		final int apartment;
	}

	@Data
	public static class Street {
		final String name;
		final int number;
	}

	@EqualsAndHashCode
	public static class ClassWithIntId {
		@Id
		public int id;
	}

	@Document(expiration = EXPIRATION_ONE_SECOND)
	@AllArgsConstructor
	@ToString
	@EqualsAndHashCode
	public static class Person {
		@Id
		String id;
		Set<Address> addresses;

		public Person() {

		}

		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	@Data
	@Document(collection = "versioned-set")
	public static class VersionedClass {

		@Id
		private String id;

		@Version
		public long version;

		public String field;

		@PersistenceConstructor
		public VersionedClass(String id, String field, long version) {
			this.id = id;
			this.field = field;
			this.version = version;
		}

		public VersionedClass(String id, String field) {
			this.id = id;
			this.field = field;
		}
	}

	@Getter
	@EqualsAndHashCode
	@ToString
	@Document(collection = "custom-set")
	public static class CustomCollectionClass {

		@Id
		private String id;
		private String data;

		public CustomCollectionClass(String id, String data) {
			this.id = id;
			this.data = data;
		}
	}

	@Data
	@AllArgsConstructor
	@Document(expiration = EXPIRATION_ONE_SECOND)
	public static class DocumentWithExpiration {

		@Id
		private String id;
	}

	@AllArgsConstructor
	@Document
	@Data
	public static class EnumProperties {

		TYPES type;
		List<TYPES> list;
		EnumSet<TYPES> set;
		EnumMap<TYPES, String> map;
	}

	@WritingConverter
	public static class ComplexIdToStringConverter implements Converter<ComplexId, String> {

		@Override
		public String convert(ComplexId complexId) {
			return "id::" + complexId.getInnerId();
		}
	}

	@ReadingConverter
	public static class StringToComplexIdConverter implements Converter<String, ComplexId> {

		@Override
		public ComplexId convert(String s) {
			long id = Long.parseLong(s.split("::")[1]);
			return new ComplexId(id);
		}
	}

	@Data
	@AllArgsConstructor
	@Document(collection = "expiration-set", expiration = 1, touchOnRead = true)
	public static class DocumentWithTouchOnRead {

		@Id
		private String id;
	}

	@Data
	@AllArgsConstructor
	@Document(collection = "expiration-set")
	public static class DocumentWithExpirationAnnotation {

		@Id
		private String id;

		@Expiration
		private Integer expiration;
	}

	@Data
	@AllArgsConstructor
	@Document(collection = "expiration-set")
	public static class DocumentWithUnixTimeExpiration {

		@Id
		private String id;

		@Expiration(unixTime = true)
		private DateTime expiration;
	}

	@Document(expirationExpression = "${expirationProperty}")
	public static class DocumentWithExpirationExpression {

	}

	@Document(expiration = EXPIRATION_ONE_SECOND, expirationUnit = TimeUnit.MINUTES)
	public static class DocumentWithExpirationUnit {

	}

	@Document
	public static class DocumentWithoutExpiration {

	}

	public static class DocumentWithoutAnnotation {

	}

	@Document(expiration = 1, expirationExpression = "${expirationProperty}")
	public static class DocumentWithExpirationAndExpression {

	}

	@Data
	@Document
	public static class DocumentWithDefaultConstructor {

		@Expiration(unixTime = true)
		private DateTime expiration;
	}
}

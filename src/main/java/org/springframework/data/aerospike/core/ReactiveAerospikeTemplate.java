package org.springframework.data.aerospike.core;

import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

import static com.aerospike.client.policy.RecordExistsAction.*;

/**
 * Primary implementation of {@link ReactiveAerospikeOperations}.
 *
 * @author Igor Ermolenko
 * @author Volodymyr Shpynta
 */
@Slf4j
public class ReactiveAerospikeTemplate extends BaseAerospikeTemplate implements ReactiveAerospikeOperations {
    private final AerospikeReactorClient reactorClient;

    public ReactiveAerospikeTemplate(AerospikeClient client,
                                     String namespace,
                                     MappingAerospikeConverter converter,
                                     AerospikeMappingContext mappingContext,
                                     AerospikeExceptionTranslator exceptionTranslator,
                                     AerospikeReactorClient reactorClient) {
        super(client, namespace, converter, mappingContext, exceptionTranslator);
        this.reactorClient = reactorClient;
    }

    @Override
    public <T> Mono<T> save(T document) {
        Assert.notNull(document, "Object to insert must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return doPersistWithCas(document, entity);
        } else {
            return doPersist(document, createWritePolicyBuilder(UPDATE));
        }
    }

    @Override
    public <T> Mono<T> persist(T document, WritePolicy policy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");
        return doPersist(document, policy);
    }

    @Override
    public <T> Flux<T> insertAll(Collection<? extends T> documents) {
        return Flux.fromIterable(documents)
                .flatMap(this::insert);
    }

    @Override
    public <T> Mono<T> insert(T document) {
        Assert.notNull(document, "Document must not be null!");
        return doPersist(document, createWritePolicyBuilder(CREATE_ONLY));
    }

    @Override
    public <T> Mono<T> update(T document) {
        Assert.notNull(document, "Document must not be null!");
        return doPersist(document, createWritePolicyBuilder(UPDATE_ONLY));
    }

    public <T> Mono<Optional<T>> findById(Serializable id, Class<T> targetType) throws AerospikeException {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(targetType, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(targetType);
        Key key = getKey(id, entity);

        return reactorClient.get(key)
                .map(keyRecord -> mapToEntityOptional(keyRecord.key, targetType, keyRecord.record));
    }

    public <T> Mono<Boolean> delete(T objectToDelete) {
        Assert.notNull(objectToDelete, "Object to delete must not be null!");

        AerospikeWriteData data = AerospikeWriteData.forWrite();
        converter.write(objectToDelete, data);

        return this.reactorClient.delete(null, data.getKey())
                .map(key -> true);
    }

    private <T> Mono<T> doPersist(T document, WritePolicyBuilder policyBuilder) {
        try {
            AerospikeWriteData data = writeData(document);
            WritePolicy policy = policyBuilder.expiration(data.getExpiration())
                    .build();
            return reactorClient.put(policy, data.getKey(), data.getBinsAsArray())
                    .map(docKey -> document);
        } catch (AerospikeException e) {
            throw translateIfPossible(e);
        }
    }

    private <T> Mono<T> doPersist(T document, WritePolicy policy) {
        try {
            AerospikeWriteData data = writeData(document);
            return reactorClient.put(policy, data.getKey(), data.getBinsAsArray())
                    .map(docKey -> document);
        } catch (AerospikeException e) {
            throw translateIfPossible(e);
        }
    }

    private <T> Mono<T> doPersistWithCas(T document, AerospikePersistentEntity<?> entity) {
        try {
            AerospikeWriteData data = writeData(document);
            ConvertingPropertyAccessor accessor = getPropertyAccessor(entity, document);
            WritePolicy policy = getCasAwareWritePolicy(data, entity, accessor);
            Operation[] operations = OperationUtils.operations(data.getBinsAsArray(), Operation::put, Operation.getHeader());
            return reactorClient.operate(policy, data.getKey(), operations)
                    .map(newKeyRecord -> {
                        accessor.setProperty(entity.getVersionProperty(), newKeyRecord.record.generation);
                        return document;
                    });
        } catch (AerospikeException e) {
            int code = e.getResultCode();
            if (code == ResultCode.KEY_EXISTS_ERROR || code == ResultCode.GENERATION_ERROR) {
                throw new OptimisticLockingFailureException("Save document with version value failed", e);
            }
            throw translateIfPossible(e);
        }
    }

    private <T> Optional<T> mapToEntityOptional(Key key, Class<T> type, Record record) {
        return record == null ? Optional.empty() : Optional.of(mapToEntity(key, type, record));
    }

    private WritePolicyBuilder createWritePolicyBuilder(RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.client.writePolicyDefault)
                .sendKey(true)
                .recordExistsAction(recordExistsAction);
    }

    private <T> AerospikeWriteData writeData(T document) {
        AerospikeWriteData data = AerospikeWriteData.forWrite();
        converter.write(document, data);
        return data;
    }

}

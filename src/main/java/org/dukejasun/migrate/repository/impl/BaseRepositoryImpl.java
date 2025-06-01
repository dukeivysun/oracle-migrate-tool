package org.dukejasun.migrate.repository.impl;

import org.dukejasun.migrate.repository.BaseRepository;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import javax.persistence.EntityManager;
import java.io.Serializable;

/**
 * @author dukedpsun
 */
@SuppressWarnings("SpringJavaConstructorAutowiringInspection")
public class BaseRepositoryImpl<T, ID extends Serializable> extends SimpleJpaRepository<T, ID> implements BaseRepository<T, ID> {
    private static final int BATCH_SIZE = 2000;
    private final EntityManager entityManager;

    public BaseRepositoryImpl(JpaEntityInformation<T, ?> entityInformation, EntityManager entityManager) {
        super(entityInformation, entityManager);
        this.entityManager = entityManager;
    }

    public BaseRepositoryImpl(Class<T> domainClass, EntityManager entityManager) {
        super(domainClass, entityManager);
        this.entityManager = entityManager;
    }

    @Override
    public <S extends T> Iterable<S> batchInsert(@NotNull Iterable<S> list) {
        int index = 0;
        for (S s : list) {
            entityManager.persist(s);
            index++;
            if (index % BATCH_SIZE == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
        if (index % BATCH_SIZE != 0) {
            entityManager.flush();
            entityManager.clear();
        }
        return list;
    }

    @Override
    public <S extends T> Iterable<S> batchUpdate(@NotNull Iterable<S> list) {
        int index = 0;
        for (S s : list) {
            entityManager.merge(s);
            index++;
            if (index % BATCH_SIZE == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
        if (index % BATCH_SIZE != 0) {
            entityManager.flush();
            entityManager.clear();
        }
        return list;
    }
}

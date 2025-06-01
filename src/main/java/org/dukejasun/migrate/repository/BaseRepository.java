package org.dukejasun.migrate.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;

/**
 * @author dukedpsun
 */
@NoRepositoryBean
public interface BaseRepository<T, ID extends Serializable> extends JpaRepository<T, ID>, JpaSpecificationExecutor<T> {
    /**
     * 批量插入
     *
     * @param var1
     * @param <S>
     * @return
     */
    <S extends T> Iterable<S> batchInsert(Iterable<S> var1);

    /**
     * 批量更新
     *
     * @param var1
     * @param <S>
     * @return
     */
    <S extends T> Iterable<S> batchUpdate(Iterable<S> var1);
}

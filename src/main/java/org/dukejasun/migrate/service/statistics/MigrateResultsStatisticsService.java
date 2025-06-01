package org.dukejasun.migrate.service.statistics;


import org.dukejasun.migrate.queue.model.EventObject;

/**
 * @author dukedpsun
 */
public interface MigrateResultsStatisticsService {

    /**
     * 统计
     *
     * @param eventObject
     * @return
     */
    String statistics(EventObject eventObject);

}

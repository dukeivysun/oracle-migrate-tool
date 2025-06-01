package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.entity.DbIncrementMetrics;
import org.dukejasun.migrate.repository.DbIncrementMetricsRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("dbIncrementMetricsHandler")
public class DbIncrementMetricsHandler {
    private final DbIncrementMetricsRepository dbIncrementMetricsRepository;

    @Autowired
    public DbIncrementMetricsHandler(DbIncrementMetricsRepository dbIncrementMetricsRepository) {
        this.dbIncrementMetricsRepository = dbIncrementMetricsRepository;
    }

    @Transactional(rollbackFor = Exception.class)
    public void save(DbIncrementMetricsDTO dbIncrementMetricsDTO) {
        if (Objects.nonNull(dbIncrementMetricsDTO)) {
            Optional<DbIncrementMetrics> optional = dbIncrementMetricsRepository.findByTaskIdAndTargetSchemaAndTargetTable(dbIncrementMetricsDTO.getTaskId(), dbIncrementMetricsDTO.getTargetSchema(), dbIncrementMetricsDTO.getTargetTable());
            DbIncrementMetrics dbIncrementMetrics;
            if (optional.isPresent()) {
                dbIncrementMetrics = optional.get();
                BeanUtils.copyProperties(dbIncrementMetricsDTO, dbIncrementMetrics);
            } else {
                dbIncrementMetrics = new DbIncrementMetrics();
                BeanUtils.copyProperties(dbIncrementMetricsDTO, dbIncrementMetrics);
                dbIncrementMetrics.setCaptureTime(Timestamp.valueOf(LocalDateTime.now()));
            }
            dbIncrementMetrics.setUpdateTime(Timestamp.valueOf(LocalDateTime.now()));
            dbIncrementMetricsRepository.save(dbIncrementMetrics);
        }
    }

    public DbIncrementMetricsDTO findMaxDeployMetrics(String taskId, Boolean capture) {
        DbIncrementMetricsDTO dbIncrementMetricsDTO = null;
        List<DbIncrementMetrics> incrementMetricsList = dbIncrementMetricsRepository.findAllByTaskId(taskId);
        if (!CollectionUtils.isEmpty(incrementMetricsList)) {
            if (capture) {
                incrementMetricsList.sort(Comparator.comparing(DbIncrementMetrics::getCaptureDelay, Comparator.nullsLast(Comparator.reverseOrder())));
            } else {
                incrementMetricsList.sort(Comparator.comparing(DbIncrementMetrics::getSinkDelay, Comparator.nullsLast(Comparator.reverseOrder())));
            }
            DbIncrementMetrics dbIncrementMetrics = incrementMetricsList.get(0);
            dbIncrementMetricsDTO = new DbIncrementMetricsDTO();
            BeanUtils.copyProperties(dbIncrementMetrics, dbIncrementMetricsDTO);
        }
        return dbIncrementMetricsDTO;
    }
}

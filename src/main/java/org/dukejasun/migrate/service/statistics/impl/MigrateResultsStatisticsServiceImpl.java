package org.dukejasun.migrate.service.statistics.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.handler.DbIncrementMetricsHandler;
import org.dukejasun.migrate.handler.DbIncrementRecordHandler;
import org.dukejasun.migrate.handler.DbMigrateResultStatisticsHandler;
import org.dukejasun.migrate.model.dto.event.DbCaptureRecordDTO;
import org.dukejasun.migrate.model.dto.event.DbIncrementMetricsDTO;
import org.dukejasun.migrate.model.dto.event.DbMigrateResultStatisticsDTO;
import org.dukejasun.migrate.model.dto.event.DbSinkRecordDTO;
import org.dukejasun.migrate.model.entity.DbIncrementRecord;
import org.dukejasun.migrate.model.entity.DbMigrateResultStatistics;
import org.dukejasun.migrate.queue.model.EventObject;
import org.dukejasun.migrate.service.statistics.MigrateResultsStatisticsService;
import org.dukejasun.migrate.utils.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("transmissionResultsStatistics")
public class MigrateResultsStatisticsServiceImpl implements MigrateResultsStatisticsService {
    private String taskId;
    private final CommonConfig commonConfig;
    private final DbIncrementRecordHandler dbIncrementRecordHandler;
    private final DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler;
    private final DbIncrementMetricsHandler dbIncrementMetricsHandler;
    private final ThreadPoolExecutor executorIncrementService;
    private final ConcurrentHashMap<String, DbCaptureRecordDTO> concurrentHashMap = new ConcurrentHashMap<>();

    @Autowired
    public MigrateResultsStatisticsServiceImpl(CommonConfig commonConfig, DbIncrementRecordHandler dbIncrementRecordHandler, DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler, DbIncrementMetricsHandler dbIncrementMetricsHandler) {
        this.commonConfig = commonConfig;
        this.dbIncrementRecordHandler = dbIncrementRecordHandler;
        this.dbMigrateResultStatisticsHandler = dbMigrateResultStatisticsHandler;
        this.dbIncrementMetricsHandler = dbIncrementMetricsHandler;
        executorIncrementService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), new ThreadFactoryBuilder().setNameFormat(Constants.CAPTURE_DATA_RECORD_THREAD_NAME).build(), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public String statistics(EventObject eventObject) {
        if (eventObject instanceof DbMigrateResultStatisticsDTO) {
            DbMigrateResultStatisticsDTO dbMigrateResultStatisticsDTO = (DbMigrateResultStatisticsDTO) eventObject;
            taskId = dbMigrateResultStatisticsDTO.getTaskId();
            DbMigrateResultStatistics dbMigrateResultStatistics = dbMigrateResultStatisticsHandler.findByUniqueId(taskId, dbMigrateResultStatisticsDTO.getTransmissionType(), dbMigrateResultStatisticsDTO.getSourceSchema(), dbMigrateResultStatisticsDTO.getSourceTable());
            if (Objects.isNull(dbMigrateResultStatistics)) {
                dbMigrateResultStatistics = new DbMigrateResultStatistics();
                dbMigrateResultStatistics.setTaskId(taskId);
                dbMigrateResultStatistics.setSourceSchema(dbMigrateResultStatisticsDTO.getSourceSchema());
                dbMigrateResultStatistics.setSourceTable(dbMigrateResultStatisticsDTO.getSourceTable());
                dbMigrateResultStatistics.setTargetSchema(dbMigrateResultStatisticsDTO.getTargetSchema());
                dbMigrateResultStatistics.setTargetTable(dbMigrateResultStatisticsDTO.getTargetTable());
                dbMigrateResultStatistics.setTransmissionType(dbMigrateResultStatisticsDTO.getTransmissionType());
                dbMigrateResultStatistics.setCreateTime(LocalDateTime.now());
            }
            if (StringUtils.isNotBlank(dbMigrateResultStatisticsDTO.getErrorReason())) {
                dbMigrateResultStatistics.setTransmissionResult(StatusEnum.ERROR.getCode());
                dbMigrateResultStatistics.setErrorReason(dbMigrateResultStatisticsDTO.getErrorReason());
            } else {
                dbMigrateResultStatistics.setTransmissionResult(StatusEnum.FINISH.getCode());
            }
            if (Objects.nonNull(dbMigrateResultStatisticsDTO.getSourceRecord())) {
                dbMigrateResultStatistics.setSourceRecord(dbMigrateResultStatisticsDTO.getSourceRecord());
            }
            switch (MigrateType.getMigrateType(dbMigrateResultStatisticsDTO.getTransmissionType())) {
                case STRUCTURE:
                    dbMigrateResultStatistics.setTargetRecord(dbMigrateResultStatisticsDTO.getTargetRecord());
                    break;
                case TOTAL:
                    long count = Long.parseLong(StringUtils.isNotBlank(dbMigrateResultStatistics.getTargetRecord()) ? dbMigrateResultStatistics.getTargetRecord() : "0");
                    dbMigrateResultStatistics.setTargetRecord(String.valueOf(count + Long.parseLong(Objects.nonNull(dbMigrateResultStatisticsDTO.getTargetRecord()) ? dbMigrateResultStatisticsDTO.getTargetRecord() : "0")));
                    break;
                default:
                    break;
            }
            dbMigrateResultStatistics.setUpdateTime(LocalDateTime.now());
            dbMigrateResultStatisticsHandler.save(dbMigrateResultStatistics);
        } else if (eventObject instanceof DbCaptureRecordDTO) {
            DbCaptureRecordDTO captureRecordDTO = (DbCaptureRecordDTO) eventObject;
            taskId = captureRecordDTO.getTaskId();
            concurrentHashMap.put(String.valueOf(captureRecordDTO.getCaptureIndex()), captureRecordDTO);
            executorIncrementService.submit(this::saveCaptureRecord);
        } else if (eventObject instanceof DbSinkRecordDTO) {
            DbSinkRecordDTO sinkRecordDTO = (DbSinkRecordDTO) eventObject;
            taskId = sinkRecordDTO.getTaskId();
            DbIncrementRecord dbIncrementRecord = dbIncrementRecordHandler.findIncrementRecord(taskId);
            if (Objects.isNull(dbIncrementRecord)) {
                dbIncrementRecord = new DbIncrementRecord();
                dbIncrementRecord.setCreateTime(LocalDateTime.now());
                dbIncrementRecord.setCaptureIndex(0);
            }
            dbIncrementRecord.setTaskId(taskId);
            dbIncrementRecord.setSinkIndex(sinkRecordDTO.getSinkIndex());
            dbIncrementRecord.setSinkStartScn(sinkRecordDTO.getSinkStartScn());
            sinkRecordDTO.setUpdateTime(LocalDateTime.now());
            dbIncrementRecordHandler.save(dbIncrementRecord);
        } else if (eventObject instanceof DbIncrementMetricsDTO) {
            dbIncrementMetricsHandler.save((DbIncrementMetricsDTO) eventObject);
        }
        return null;
    }

    /**
     * 异步写入增量采集统计结果
     */
    private void saveCaptureRecord() {
        synchronized (this) {
            int captureNumber = commonConfig.getIncrementCaptureNumber();
            while (!CollectionUtils.isEmpty(concurrentHashMap)) {
                String parentDir = commonConfig.getOutputPath() + File.separator + MigrateType.INCREMENT.getCode().toLowerCase() + File.separator + taskId;
                String keyword = commonConfig.getIncrementDataFilePrefix() + captureNumber + '@';
                boolean isExists = !FileUtil.searchFiles(parentDir, keyword, Constants.DATA_SUFFIX).isEmpty();
                while (!isExists) {
                    isExists = !FileUtil.searchFiles(parentDir, keyword, Constants.DATA_SUFFIX).isEmpty();
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000L);
                    } catch (InterruptedException ignored) {
                    }
                }
                DbCaptureRecordDTO dbCaptureRecordDTO = concurrentHashMap.get(String.valueOf(captureNumber));
                if (Objects.nonNull(dbCaptureRecordDTO)) {
                    DbIncrementRecord dbIncrementRecord = dbIncrementRecordHandler.findIncrementRecord(taskId);
                    if (Objects.isNull(dbIncrementRecord)) {
                        dbIncrementRecord = new DbIncrementRecord();
                        dbIncrementRecord.setCreateTime(LocalDateTime.now());
                        dbIncrementRecord.setSinkIndex(0);
                    }
                    dbCaptureRecordDTO.setUpdateTime(LocalDateTime.now());
                    dbIncrementRecord.setTaskId(taskId);
                    dbIncrementRecord.setCaptureIndex(dbCaptureRecordDTO.getCaptureIndex());
                    dbIncrementRecord.setCaptureStartScn(dbCaptureRecordDTO.getCaptureStartScn());
                    dbIncrementRecord.setCaptureEndScn(dbCaptureRecordDTO.getCaptureEndScn());
                    dbIncrementRecordHandler.save(dbIncrementRecord);
                    concurrentHashMap.remove(String.valueOf(captureNumber));
                    captureNumber += 1;
                    commonConfig.setIncrementCaptureNumber(captureNumber);
                    log.info(Constants.FINISH_INCREMENT_MESSAGE, taskId, dbCaptureRecordDTO.getCaptureIndex(), dbCaptureRecordDTO.getCaptureStartScn(), dbCaptureRecordDTO.getCaptureEndScn());
                }
            }
        }
    }
}

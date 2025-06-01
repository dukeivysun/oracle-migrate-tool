package org.dukejasun.migrate.config;

import org.dukejasun.migrate.cache.LocalCache;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.cache.local.CompareResultCache;
import org.dukejasun.migrate.cache.local.TaskCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.handler.DbIncrementRecordHandler;
import org.dukejasun.migrate.handler.DbMigrateResultStatisticsHandler;
import org.dukejasun.migrate.handler.DbMigrateTaskHandler;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.CompareResultDTO;
import org.dukejasun.migrate.model.entity.DbIncrementRecord;
import org.dukejasun.migrate.model.entity.DbMigrateResultStatistics;
import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.utils.OutputLogWithTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Configuration("terminateConfig")
public class TerminateConfig {

    private final CommonConfig commonConfig;
    private final DbMigrateTaskHandler dbMigrateTaskHandler;
    private final DbIncrementRecordHandler dbIncrementRecordHandler;
    private final DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler;

    @Autowired
    public TerminateConfig(CommonConfig commonConfig, DbIncrementRecordHandler dbIncrementRecordHandler, DbMigrateTaskHandler dbMigrateTaskHandler, DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler) {
        this.commonConfig = commonConfig;
        this.dbIncrementRecordHandler = dbIncrementRecordHandler;
        this.dbMigrateTaskHandler = dbMigrateTaskHandler;
        this.dbMigrateResultStatisticsHandler = dbMigrateResultStatisticsHandler;
    }

    @Bean
    public TerminateTaskBean terminateTaskBean() {
        return new TerminateTaskBean(commonConfig.getStartTransmissionTime(), dbIncrementRecordHandler, dbMigrateTaskHandler, dbMigrateResultStatisticsHandler);
    }

    static class TerminateTaskBean {
        private final LocalDateTime startTime;
        private final DbIncrementRecordHandler dbIncrementRecordHandler;
        private final DbMigrateTaskHandler dbMigrateTaskHandler;
        private final DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler;

        public TerminateTaskBean(@NotNull LocalDateTime startTransmissionTime, DbIncrementRecordHandler dbIncrementRecordHandler, DbMigrateTaskHandler dbMigrateTaskHandler, DbMigrateResultStatisticsHandler dbMigrateResultStatisticsHandler) {
            this.startTime = startTransmissionTime;
            this.dbIncrementRecordHandler = dbIncrementRecordHandler;
            this.dbMigrateTaskHandler = dbMigrateTaskHandler;
            this.dbMigrateResultStatisticsHandler = dbMigrateResultStatisticsHandler;
        }

        @PreDestroy
        public void preDestroy() {
            LocalCache<String, String> localCache = TaskCache.INSTANCE.getAll();
            if (!CollectionUtils.isEmpty(localCache)) {
                localCache.forEach((key, value) -> {
                    DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findByIdAndProcessId(key, value);
                    if (Objects.nonNull(dbMigrateTask)) {
                        dbMigrateTask.setUpdateTime(LocalDateTime.now());
                        dbMigrateTask.setComments("");
                        dbMigrateTask.setIsDeleted(false);
                        dbMigrateTask.setStatus(StatusEnum.FINISH.getCode());
                        dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);
                        switch (MigrateType.getMigrateType(dbMigrateTask.getMigrateType())) {
                            case TOTAL:
                                printTotalStatistics(dbMigrateTask.getId());
                                printDataCompareResult(dbMigrateTask.getId());
                                break;
                            case STRUCTURE:
                                printStructureStatistics(dbMigrateTask.getId());
                                break;
                            case INCREMENT:
                                printIncrementStatistics(dbMigrateTask.getId());
                            default:
                                break;
                        }
                    }
                });
            }
        }

        private void printIncrementStatistics(Integer taskId) {
            Integer captureIndex = 0;
            Integer sinkIndex = 0;
            DbIncrementRecord dbIncrementRecord = dbIncrementRecordHandler.findIncrementRecord(String.valueOf(taskId));
            if (Objects.nonNull(dbIncrementRecord)) {
                captureIndex = dbIncrementRecord.getCaptureIndex();
                sinkIndex = dbIncrementRecord.getSinkIndex();
            }
            log.info(Constants.INCREMENT_RESULT_STATISTICS, taskId, captureIndex, sinkIndex, (System.currentTimeMillis() - startTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()) / 1000);
        }

        private void printDataCompareResult(Integer taskId) {
            List<CompareResultDTO> compareResultDTOList = CompareResultCache.INSTANCE.get(String.valueOf(taskId));
            if (!CollectionUtils.isEmpty(compareResultDTOList)) {
                OutputLogWithTable outputLogWithTable = OutputLogWithTable.create("全量迁移结果验证如下:");
                outputLogWithTable.setSbcMode(false);
                outputLogWithTable.addHeader("表名", "对比条件", "数据源行数", "目标端行数", "验证结果", "差异数据");
                compareResultDTOList.forEach(compareResultDTO -> outputLogWithTable.addBody(compareResultDTO.getSchema() + '.' + compareResultDTO.getTableName(), "", compareResultDTO.getSourceNumber(), compareResultDTO.getTargetNumber(), compareResultDTO.getCompareResult(), ""));
                outputLogWithTable.printToConsole();
            }
        }

        private void printTotalStatistics(Integer taskId) {
            List<DbMigrateResultStatistics> dbMigrateResultStatisticsList = dbMigrateResultStatisticsHandler.findAllByTaskIdAndTransmissionType(String.valueOf(taskId), MigrateType.TOTAL.getCode());
            if (!CollectionUtils.isEmpty(dbMigrateResultStatisticsList)) {
                OutputLogWithTable outputLogWithTable = OutputLogWithTable.create("全量迁移结果统计如下:");
                outputLogWithTable.setSbcMode(false);
                outputLogWithTable.addHeader("", "表名", "表数据量", "导出文件数", "导出结果", "开始时间", "结束时间", "迁移用时", "迁移速度");
                AtomicInteger atomicInteger = new AtomicInteger();
                AtomicInteger count = new AtomicInteger(1);
                Map<String, DbMigrateResultStatistics> stringListMap = dbMigrateResultStatisticsList.stream().collect(Collectors.toMap(dbMigrateResultStatistics -> MessageFormat.format(Constants.SCHEMA_TABLE_KEY, dbMigrateResultStatistics.getSourceSchema(), dbMigrateResultStatistics.getSourceTable()), Function.identity(), (key1, key2) -> key2));
                stringListMap.forEach((key1, dbMigrateResultStatistics) -> {
                    CaptureTableDTO captureTableDTO = CaptureTableCache.INSTANCE.get(key1);
                    if (Objects.nonNull(captureTableDTO)) {
                        if (StatusEnum.ERROR.getCode().equals(dbMigrateResultStatistics.getTransmissionResult())) {
                            atomicInteger.incrementAndGet();
                        }
                        LocalDateTime createTime = captureTableDTO.getStartTime();
                        LocalDateTime endTime = captureTableDTO.getEndTime();
                        long second = Duration.between(createTime, endTime).getSeconds();
                        long sourceRecord = 0L;
                        try {
                            sourceRecord = Long.parseLong(dbMigrateResultStatistics.getSourceRecord());
                        } catch (NumberFormatException ignored) {
                        }
                        second = second == 0 ? 1L : second;
                        long speed = sourceRecord / second;
                        outputLogWithTable.addBody(String.valueOf(count.get()), key1, Objects.nonNull(dbMigrateResultStatistics.getSourceRecord()) ? dbMigrateResultStatistics.getSourceRecord() : "0", Objects.nonNull(dbMigrateResultStatistics.getTargetRecord()) ? dbMigrateResultStatistics.getTargetRecord() : "0", StringUtils.isNotBlank(dbMigrateResultStatistics.getTransmissionResult()) ? dbMigrateResultStatistics.getTransmissionResult() : "FINISH", createTime.toString(), endTime.toString(), second + "秒", speed + " 条/秒");
                        count.incrementAndGet();
                    }
                });
                outputLogWithTable.addTail("汇总", "迁移对象数量:" + dbMigrateResultStatisticsList.size(), "成功数量:" + (dbMigrateResultStatisticsList.size() - atomicInteger.intValue()), "失败数量:" + atomicInteger.intValue(), "开始时间:" + startTime, "结束时间:" + LocalDateTime.now(), "迁移总耗时:" + (System.currentTimeMillis() - startTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()) / 1000 + "秒");
                outputLogWithTable.printToConsole();
            }
        }

        private void printStructureStatistics(Integer taskId) {
            List<DbMigrateResultStatistics> dbMigrateResultStatisticsList = dbMigrateResultStatisticsHandler.findAllByTaskIdAndTransmissionType(String.valueOf(taskId), MigrateType.STRUCTURE.getCode());
            if (!CollectionUtils.isEmpty(dbMigrateResultStatisticsList)) {
                AtomicInteger allErrorNumber = new AtomicInteger();
                Map<String, List<DbMigrateResultStatistics>> stringListMap = dbMigrateResultStatisticsList.stream().collect(Collectors.groupingBy(DbMigrateResultStatistics::getSourceSchema));
                stringListMap.forEach((key2, value2) -> {
                    Map<String, List<DbMigrateResultStatistics>> statusListMap = value2.stream().collect(Collectors.groupingBy(DbMigrateResultStatistics::getTransmissionResult));
                    int errorSize = !CollectionUtils.isEmpty(statusListMap.get(StatusEnum.ERROR.getCode())) ? statusListMap.get(StatusEnum.ERROR.getCode()).size() : 0;
                    int finishSize = !CollectionUtils.isEmpty(statusListMap.get(StatusEnum.FINISH.getCode())) ? statusListMap.get(StatusEnum.FINISH.getCode()).size() : 0;
                    allErrorNumber.addAndGet(errorSize);
                    log.info(Constants.STRUCTURE_RESULT_STATISTICS, key2, value2.size(), finishSize, errorSize);
                });
                log.info(Constants.TRANSMISSION_RESULT_STATISTICS, dbMigrateResultStatisticsList.size(), dbMigrateResultStatisticsList.size() - allErrorNumber.intValue(), allErrorNumber.intValue(), (System.currentTimeMillis() - startTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()) / 1000);
            }
        }
    }
}

package org.dukejasun.migrate.service.compare.total;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.dukejasun.migrate.cache.local.CompareResultCache;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.common.IConstants;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.dto.output.CompareResultDTO;
import org.dukejasun.migrate.service.compare.CompareService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author dukedpsun
 */
@Slf4j
public class TotalDataCompareImpl implements Callable<Object>, CompareService {
    private final String taskId;
    private final CaptureTableDTO captureTableDTO;
    private final DatabaseFeatures databaseFeatures;
    private final DatabaseFeatures targetDatabaseFeatures;
    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    public TotalDataCompareImpl(String taskId, CaptureTableDTO captureTableDTO, DatabaseFeatures databaseFeatures, DatabaseFeatures targetDatabaseFeatures, JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate) {
        this.taskId = taskId;
        this.captureTableDTO = captureTableDTO;
        this.databaseFeatures = databaseFeatures;
        this.targetDatabaseFeatures = targetDatabaseFeatures;
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }

    @Override
    public Object call() {
        log.info("开始源端{}与目标端{}【{}.{}】表数据验证......", databaseFeatures.databaseType().name(), targetDatabaseFeatures.databaseType().name(), captureTableDTO.getSchema(), captureTableDTO.getName());
        final CompareResultDTO compareResultDTO = new CompareResultDTO();
        compareResultDTO.setTaskId(taskId);
        compareResultDTO.setSchema(captureTableDTO.getSchema());
        compareResultDTO.setTableName(captureTableDTO.getName());
        compareResultDTO.setSourceNumber("0");
        compareResultDTO.setTargetNumber("0");

        final CyclicBarrier cyclicBarrier = new CyclicBarrier(2, () -> {
            compareResultDTO.setCompareResult(compareResultDTO.getSourceNumber().equals(compareResultDTO.getTargetNumber()) ? "通过" : "不通过");
            CompareResultCache.INSTANCE.put(taskId, compareResultDTO);
            captureTableDTO.setEndTime(LocalDateTime.now());
            captureTableDTO.getLongAdder().increment();
        });
        ExecutorService executorService = new ThreadPoolExecutor(2, 2, 10, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat(MessageFormat.format(Constants.COMPARE_TABLE_SUB_THREAD_NAME, captureTableDTO.getSchema(), captureTableDTO.getName())).build(), new ThreadPoolExecutor.CallerRunsPolicy());
        handlerCompareResult(executorService, compareResultDTO, cyclicBarrier, sourceJdbcTemplate, databaseFeatures);
        handlerCompareResult(executorService, compareResultDTO, cyclicBarrier, targetJdbcTemplate, targetDatabaseFeatures);
        return null;
    }

    private void handlerCompareResult(@NotNull ExecutorService executorService, final CompareResultDTO compareResultDTO, CyclicBarrier cyclicBarrier, JdbcTemplate jdbcTemplate, DatabaseFeatures databaseFeatures) {
        executorService.submit(new TableCompareCallable(captureTableDTO, compareResultDTO, jdbcTemplate, databaseFeatures, cyclicBarrier));
    }

    static class TableCompareCallable implements Callable<CompareResultDTO> {
        private final CaptureTableDTO captureTableDTO;
        private final CompareResultDTO compareResultDTO;
        private final JdbcTemplate jdbcTemplate;
        private final DatabaseFeatures databaseFeatures;
        private final CyclicBarrier cyclicBarrier;

        public TableCompareCallable(CaptureTableDTO captureTableDTO, CompareResultDTO compareResultDTO, JdbcTemplate jdbcTemplate, DatabaseFeatures databaseFeatures, CyclicBarrier cyclicBarrier) {
            this.captureTableDTO = captureTableDTO;
            this.compareResultDTO = compareResultDTO;
            this.jdbcTemplate = jdbcTemplate;
            this.databaseFeatures = databaseFeatures;
            this.cyclicBarrier = cyclicBarrier;
        }

        @Override
        public CompareResultDTO call() throws Exception {
            try {
                String querySql = MessageFormat.format(IConstants.QUERY_TABLE_NUMBERS, databaseFeatures.getSchema(captureTableDTO.getSchema()), databaseFeatures.getTableName(captureTableDTO.getName()));
                if (StringUtils.isNotBlank(captureTableDTO.getConditions())) {
                    querySql = MessageFormat.format(IConstants.QUERY_TABLE_NUMBERS_WITH_CONDITIONS, databaseFeatures.getSchema(captureTableDTO.getSchema()), databaseFeatures.getTableName(captureTableDTO.getName()), captureTableDTO.getConditions());
                }
                Long count = jdbcTemplate.queryForObject(querySql, Long.class);
                if (Objects.nonNull(count)) {
                    if (databaseFeatures.databaseType().equals(DatabaseExpandType.ORACLE)) {
                        compareResultDTO.setSourceNumber(String.valueOf(count));
                    } else {
                        compareResultDTO.setTargetNumber(String.valueOf(count));
                    }
                }
                return compareResultDTO;
            } catch (Exception e) {
                log.warn("数据对比异常", e);
            } finally {
                cyclicBarrier.await();
            }
            return null;
        }
    }
}

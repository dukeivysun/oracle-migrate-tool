package org.dukejasun.migrate.manager;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.dukejasun.migrate.cache.local.CaptureTableCache;
import org.dukejasun.migrate.cache.local.TableMetadataCache;
import org.dukejasun.migrate.cache.local.TaskCache;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.common.Constants;
import org.dukejasun.migrate.enums.DataSourceFactory;
import org.dukejasun.migrate.enums.DatabaseExpandType;
import org.dukejasun.migrate.enums.MigrateType;
import org.dukejasun.migrate.enums.StatusEnum;
import org.dukejasun.migrate.features.DatabaseFeatures;
import org.dukejasun.migrate.handler.DbDatasourceHandler;
import org.dukejasun.migrate.handler.DbDatasourceParametersHandler;
import org.dukejasun.migrate.handler.DbIncrementRecordHandler;
import org.dukejasun.migrate.handler.DbMigrateTaskHandler;
import org.dukejasun.migrate.model.dto.entity.DatasourceDTO;
import org.dukejasun.migrate.model.dto.input.CaptureTableSettingDTO;
import org.dukejasun.migrate.model.dto.input.ParameterDTO;
import org.dukejasun.migrate.model.dto.metadata.CaptureTableDTO;
import org.dukejasun.migrate.model.entity.DbDatasource;
import org.dukejasun.migrate.model.entity.DbDatasourceParameters;
import org.dukejasun.migrate.model.entity.DbIncrementRecord;
import org.dukejasun.migrate.model.entity.DbMigrateTask;
import org.dukejasun.migrate.model.vo.DatasourceVO;
import org.dukejasun.migrate.model.vo.ParameterVO;
import org.dukejasun.migrate.service.capture.MigrateCaptureService;
import org.dukejasun.migrate.utils.EncryptUtil;
import org.dukejasun.migrate.utils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.system.ApplicationPid;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author dukedpsun
 */
@Slf4j
@Component("replicatorManager")
public class ReplicatorManager {
    private MigrateCaptureService migrateCaptureService;
    private final CommonConfig commonConfig;
    private final ApplicationContext applicationContext;
    private final DbMigrateTaskHandler dbMigrateTaskHandler;
    private final DbDatasourceHandler dbDatasourceHandler;
    private final DbDatasourceParametersHandler dbDatasourceParametersHandler;
    private final DbIncrementRecordHandler dbIncrementRecordHandler;
    private final Map<String, DatabaseFeatures> databaseFeaturesMap;
    private final Map<String, MigrateCaptureService> migrateServiceMap;

    @Autowired
    public ReplicatorManager(CommonConfig commonConfig, ApplicationContext applicationContext, DbMigrateTaskHandler dbMigrateTaskHandler, DbDatasourceHandler dbDatasourceHandler, DbDatasourceParametersHandler dbDatasourceParametersHandler, DbIncrementRecordHandler dbIncrementRecordHandler, Map<String, DatabaseFeatures> databaseFeaturesMap, Map<String, MigrateCaptureService> migrateServiceMap) {
        this.commonConfig = commonConfig;
        this.applicationContext = applicationContext;
        this.dbMigrateTaskHandler = dbMigrateTaskHandler;
        this.dbDatasourceHandler = dbDatasourceHandler;
        this.dbDatasourceParametersHandler = dbDatasourceParametersHandler;
        this.dbIncrementRecordHandler = dbIncrementRecordHandler;
        this.databaseFeaturesMap = databaseFeaturesMap;
        this.migrateServiceMap = migrateServiceMap;
    }

    /**
     * 启动已经存在的任务
     *
     * @param taskId
     */
    public void startTaskById(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            throw new RuntimeException("未正确指定需要运行taskId.启动失败!");
        }
        DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findMigrateTaskByTaskId(taskId);
        if (Objects.nonNull(dbMigrateTask)) {
            if ((!dbMigrateTask.getIsDeleted() && StringUtils.isBlank(dbMigrateTask.getComments())) || commonConfig.getReplicateType() == 2) {
                log.info("开始【{}】迁移......", MigrateType.getMigrateType(dbMigrateTask.getMigrateType()).getValue());
                migrateCaptureService = migrateServiceMap.get(dbMigrateTask.getMigrateType() + "_TRANSMISSION_SERVICE");
                ExecutorService singleThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                        new ThreadFactoryBuilder().setNameFormat(Constants.TRANSMISSION_THREAD_NAME).build(), new ThreadPoolExecutor.AbortPolicy());
                singleThreadPool.execute(() -> {
                    try {
                        String sourceId = dbMigrateTask.getSourceId();
                        String targetId = dbMigrateTask.getTargetId();
                        DatasourceDTO dataSourceDTO = generatorDatasourceDTO(null, sourceId);
                        DatasourceDTO targetSourceDTO = generatorDatasourceDTO(null, targetId);
                        if (Objects.nonNull(dataSourceDTO) || Objects.nonNull(targetSourceDTO)) {
                            DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(dataSourceDTO, commonConfig);
                            if (Objects.nonNull(dataSource)) {
                                JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                                List<String> schemaList = Lists.newArrayList(StringUtils.split(dbMigrateTask.getCaptureSchemas(), ','));
                                StringBuilder bufferSchema = new StringBuilder();
                                for (Iterator<String> iterator = schemaList.iterator(); iterator.hasNext(); ) {
                                    String schemaName = iterator.next();
                                    bufferSchema.append('\'').append(schemaName.toUpperCase()).append('\'');
                                    if (iterator.hasNext()) {
                                        bufferSchema.append(',');
                                    }
                                }
                                if (commonConfig.getReplicateType() != 2) {
                                    dbMigrateTask.setComments(new ApplicationPid().toString());
                                    dbMigrateTask.setUpdateTime(LocalDateTime.now());
                                    dbMigrateTask.setStatus(StatusEnum.RUNNING.getCode());
                                    dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);
                                }
                                Map<String, Object> objectMap = Objects.requireNonNull(JacksonUtil.fromJson(dbMigrateTask.getExtra(), Map.class));
                                String captureTableSettingPath = Objects.nonNull(objectMap.get("captureTableSettingPath")) ? (String) objectMap.get("captureTableSettingPath") : "";
                                CaptureTableSettingDTO captureTableSettingDTO = null;
                                if (StringUtils.isNotBlank(captureTableSettingPath)) {
                                    captureTableSettingDTO = generatorCaptureTableSetting(captureTableSettingPath);
                                }
                                TaskCache.INSTANCE.put(taskId, dbMigrateTask.getComments());
                                initCaptureTableMetadata(taskId, jdbcTemplate, bufferSchema.toString(), captureTableSettingDTO, dbMigrateTask.getMigrateType(), dataSourceDTO.getType().name(), dataSourceDTO);

                                ParameterDTO parameterDTO = new ParameterDTO();
                                parameterDTO.setTaskId(taskId);
                                parameterDTO.setMigrateType(MigrateType.getMigrateType(dbMigrateTask.getMigrateType()));
                                parameterDTO.setDataSourceDTO(dataSourceDTO);
                                parameterDTO.setTargetSourceDTO(targetSourceDTO);
                                parameterDTO.setSchemaList(Arrays.asList(StringUtils.split(dbMigrateTask.getCaptureSchemas(), Constants.COMMA)));
                                MigrateType migrateType = parameterDTO.getMigrateType();
                                switch (migrateType) {
                                    case INCREMENT:
                                        if (!CollectionUtils.isEmpty(objectMap)) {
                                            settingParameterExtra(parameterDTO, objectMap);
                                        }
                                        DbIncrementRecord dbIncrementRecord = dbIncrementRecordHandler.findIncrementRecord(taskId);
                                        if (Objects.nonNull(dbIncrementRecord)) {
                                            parameterDTO.setStartScn(Long.valueOf(StringUtils.isNotBlank(dbIncrementRecord.getCaptureEndScn()) ? dbIncrementRecord.getCaptureEndScn() : String.valueOf(parameterDTO.getStartScn())));
                                            Map<String, Object> extra = parameterDTO.getExtra();
                                            if (CollectionUtils.isEmpty(extra)) {
                                                extra = Maps.newHashMap();
                                                parameterDTO.setExtra(extra);
                                            }
                                            extra.put("index", dbIncrementRecord.getCaptureIndex());
                                            extra.put("captureStartScn", dbIncrementRecord.getCaptureStartScn());
                                            extra.put("captureEndScn", dbIncrementRecord.getCaptureEndScn());
                                            extra.put("sinkIndex", dbIncrementRecord.getSinkIndex());
                                            extra.put("sinkEndScn", dbIncrementRecord.getSinkEndScn());
                                            extra.put("sinkStartScn", dbIncrementRecord.getSinkStartScn());
                                            parameterDTO.setStartScn(Long.valueOf(StringUtils.isNotBlank(dbIncrementRecord.getCaptureStartScn()) ? dbIncrementRecord.getCaptureStartScn() : String.valueOf(parameterDTO.getStartScn())));
                                            commonConfig.setIncrementStartNumber(dbIncrementRecord.getSinkIndex());
                                        }
                                        String totalCurrentScn = totalTaskStartScn();
                                        if (StringUtils.isNotBlank(totalCurrentScn)) {
                                            parameterDTO.setStartScn(Long.valueOf(totalCurrentScn));
                                        }
                                        migrateCaptureService.startMigrateTask(taskId, parameterDTO, jdbcTemplate);
                                        break;
                                    case STRUCTURE:
                                    case TOTAL:
                                        migrateCaptureService.startMigrateTask(taskId, parameterDTO, jdbcTemplate);
                                        migrateCaptureService.waiteUntilFinish(TableMetadataCache.INSTANCE.get());
                                        terminate(taskId);
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        try {
                            log.error("【{}】迁移任务失败!", MigrateType.getMigrateType(dbMigrateTask.getMigrateType()).getValue(), e);
                            terminate(taskId);
                        } catch (IOException ignored) {
                        }
                    }
                });
                singleThreadPool.shutdown();
            } else {
                throw new RuntimeException(MessageFormat.format("任务Id:【{0}】当前任务状态是:【{1}】,不是【FINISH】,【ERROR】或者【NOT_START】不能启动!", taskId, dbMigrateTask.getStatus()));
            }
        } else {
            throw new RuntimeException("Id为【" + taskId + "】的任务不存在.启动失败!");
        }
    }

    private @Nullable String totalTaskStartScn() {
        String totalTaskId = commonConfig.getTotalTaskId();
        if (StringUtils.isNotBlank(totalTaskId)) {
            DbMigrateTask dbMigrateTask = dbMigrateTaskHandler.findMigrateTaskByTaskId(totalTaskId);
            if (Objects.nonNull(dbMigrateTask)) {
                Map<String, Object> objectMap = Objects.requireNonNull(JacksonUtil.fromJson(dbMigrateTask.getExtra(), Map.class));
                if (!CollectionUtils.isEmpty(objectMap)) {
                    Object currentScn = objectMap.get("currentScn");
                    if (currentScn instanceof Long) {
                        return String.valueOf(currentScn);
                    }
                    return (String) currentScn;
                }
            }
        }
        return null;
    }

    private void settingParameterExtra(@NotNull ParameterDTO parameterDTO, @NotNull Map<String, Object> map) {
        parameterDTO.setStartScn(Objects.nonNull(map.get("startScn")) ? Long.parseLong((String) map.get("startScn")) : 0L);
        parameterDTO.setBatch(Objects.nonNull(map.get("batch")) ? Integer.parseInt((String) map.get("batch")) : 1000);
        parameterDTO.setDictionaryPath(Objects.nonNull(map.get("dictionaryPath")) ? (String) map.get("dictionaryPath") : "");
        parameterDTO.setDictionaryLocation(Objects.nonNull(map.get("dictionaryLocation")) ? (String) map.get("dictionaryLocation") : "");
        parameterDTO.setPdbName(Objects.nonNull(map.get("pdbName")) ? (String) map.get("pdbName") : "");
    }

    /**
     * 启动迁移任务
     *
     * @param parameterVO
     * @return
     */
    public String startMigrateTask(@NotNull ParameterVO parameterVO) {
        log.info("开始【{}】迁移......", MigrateType.getMigrateType(parameterVO.getMigrateType()).getValue());
        String sourceId = parameterVO.getSourceId();
        String targetId = parameterVO.getTargetId();
        DatasourceDTO dataSourceDTO;
        DatasourceDTO targetSourceDTO;
        if (StringUtils.isNotBlank(sourceId) && StringUtils.isNotBlank(targetId)) {
            dataSourceDTO = generatorDatasourceDTO(null, sourceId);
            targetSourceDTO = generatorDatasourceDTO(null, targetId);
        } else {
            DatasourceVO dataSourceVO = parameterVO.getDatasourceVO();
            DatasourceVO targetSourceVO = parameterVO.getTargetSourceVO();
            if (Objects.nonNull(dataSourceVO) && Objects.nonNull(targetSourceVO)) {
                sourceId = createDatasource(dataSourceVO);
                targetId = createDatasource(targetSourceVO);
                parameterVO.setSourceId(sourceId);
                parameterVO.setTargetId(targetId);
                dataSourceDTO = generatorDatasourceDTO(dataSourceVO, sourceId);
                targetSourceDTO = generatorDatasourceDTO(targetSourceVO, targetId);
            } else {
                targetSourceDTO = null;
                dataSourceDTO = null;
            }
        }

        if (Objects.nonNull(dataSourceDTO) && Objects.nonNull(targetSourceDTO)) {
            DbMigrateTask dbMigrateTask = createMigrateTask(parameterVO);
            String taskId = String.valueOf(dbMigrateTask.getId());
            TaskCache.INSTANCE.put(taskId, dbMigrateTask.getComments());
            ExecutorService singleThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat(Constants.TRANSMISSION_THREAD_NAME).build(), new ThreadPoolExecutor.AbortPolicy());
            StringBuffer bufferSchema = new StringBuffer();
            for (Iterator<String> iterator = parameterVO.getSchemaList().iterator(); iterator.hasNext(); ) {
                String schemaName = iterator.next();
                bufferSchema.append('\'').append(schemaName.toUpperCase()).append('\'');
                if (iterator.hasNext()) {
                    bufferSchema.append(',');
                }
            }
            migrateCaptureService = migrateServiceMap.get(dbMigrateTask.getMigrateType() + "_TRANSMISSION_SERVICE");
            singleThreadPool.execute(() -> {
                try {
                    DataSource dataSource = DataSourceFactory.INSTANCE.getDataSource(dataSourceDTO, commonConfig);
                    if (Objects.nonNull(dataSource)) {
                        String captureTableSettingPath = CollectionUtils.isEmpty(parameterVO.getExtra()) ? "" : Objects.nonNull(parameterVO.getExtra().get("captureTableSettingPath")) ? (String) parameterVO.getExtra().get("captureTableSettingPath") : "";
                        CaptureTableSettingDTO captureTableSettingDTO = null;
                        if (StringUtils.isNotBlank(captureTableSettingPath)) {
                            captureTableSettingDTO = generatorCaptureTableSetting(captureTableSettingPath);
                        }
                        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                        initCaptureTableMetadata(taskId, jdbcTemplate, bufferSchema.toString(), captureTableSettingDTO, parameterVO.getMigrateType(), dataSourceDTO.getType().name(), dataSourceDTO);
                        bufferSchema.setLength(0);
                        ParameterDTO parameterDTO = new ParameterDTO();
                        parameterDTO.setTaskId(taskId);
                        parameterDTO.setMigrateType(MigrateType.getMigrateType(parameterVO.getMigrateType()));
                        parameterDTO.setDataSourceDTO(dataSourceDTO);
                        parameterDTO.setTargetSourceDTO(targetSourceDTO);
                        parameterDTO.setSchemaList(parameterVO.getSchemaList());
                        MigrateType migrateType = parameterDTO.getMigrateType();
                        switch (migrateType) {
                            case INCREMENT:
                                Map<String, Object> map = parameterVO.getExtra();
                                if (!CollectionUtils.isEmpty(map)) {
                                    settingParameterExtra(parameterDTO, map);
                                }
                                String totalCurrentScn = totalTaskStartScn();
                                if (StringUtils.isNotBlank(totalCurrentScn)) {
                                    parameterDTO.setStartScn(Long.valueOf(totalCurrentScn));
                                }
                                migrateCaptureService.startMigrateTask(taskId, parameterDTO, jdbcTemplate);
                                break;
                            case STRUCTURE:
                            case TOTAL:
                                migrateCaptureService.startMigrateTask(taskId, parameterDTO, jdbcTemplate);
                                migrateCaptureService.waiteUntilFinish(TableMetadataCache.INSTANCE.get());
                                terminate(taskId);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    try {
                        log.error("【{}】迁移任务失败!", MigrateType.getMigrateType(parameterVO.getMigrateType()).getValue(), e);
                        terminate(taskId);
                    } catch (IOException ignored) {
                    }
                }
            });
            singleThreadPool.shutdown();
            return taskId;
        }
        throw new RuntimeException(MessageFormat.format("启动【{}】迁移失败!", MigrateType.getMigrateType(parameterVO.getMigrateType()).getValue()));
    }

    private @Nullable CaptureTableSettingDTO generatorCaptureTableSetting(String captureTableSettingPath) throws IOException {
        File file = new File(captureTableSettingPath);
        if (file.exists()) {
            String data = Files.asCharSource(file, Charset.defaultCharset()).readLines(new LineProcessor<String>() {
                final StringBuilder builder = new StringBuilder();

                @Override
                public boolean processLine(@NotNull String line) {
                    if (StringUtils.isNotBlank(line)) {
                        if (line.trim().getBytes()[0] != '#') {
                            builder.append(line).append('\n');
                        }
                    }
                    return true;
                }

                @Override
                public String getResult() {
                    if (builder.length() > 0) {
                        return builder.toString();
                    }
                    return null;
                }
            });
            if (Objects.nonNull(data)) {
                return JacksonUtil.fromJson(data, CaptureTableSettingDTO.class);
            }
        }
        return null;
    }

    private @NotNull DbMigrateTask createMigrateTask(@NotNull ParameterVO parameterVO) {
        List<String> schemaList = parameterVO.getSchemaList();
        if (!CollectionUtils.isEmpty(schemaList)) {
            DbMigrateTask dbMigrateTask = new DbMigrateTask();
            dbMigrateTask.setJobId(parameterVO.getJobId());
            dbMigrateTask.setTaskName(parameterVO.getTaskName());
            dbMigrateTask.setSourceId(parameterVO.getSourceId());
            dbMigrateTask.setTargetId(parameterVO.getTargetId());
            dbMigrateTask.setCaptureSchemas(Joiner.on(Constants.COMMA).join(schemaList));
            dbMigrateTask.setStatus(StatusEnum.NOT_START.getCode());
            dbMigrateTask.setMigrateType(parameterVO.getMigrateType().toUpperCase());
            dbMigrateTask.setComments(new ApplicationPid().toString());
            dbMigrateTask.setExtra(JacksonUtil.toJson(parameterVO.getExtra()));
            dbMigrateTaskHandler.saveMigrateTask(dbMigrateTask);
            return dbMigrateTask;
        }
        throw new RuntimeException("采集列表为空!");
    }

    private @Nullable DatasourceDTO generatorDatasourceDTO(DatasourceVO dataSourceVO, String sourceId) {
        if (Objects.isNull(dataSourceVO)) {
            DbDatasource dbDatasource = dbDatasourceHandler.findDatasourceById(sourceId);
            if (Objects.nonNull(dbDatasource)) {
                DatasourceDTO dataSourceDTO = new DatasourceDTO();
                dataSourceDTO.setName(dbDatasource.getDatasourceName()).setDataSourceId(String.valueOf(dbDatasource.getId())).setVersion(dbDatasource.getVersion()).setUsername(dbDatasource.getUserName()).setPassword(EncryptUtil.decodePassWord(dbDatasource.getPassword())).setHost(dbDatasource.getHost()).setPort(Integer.valueOf(dbDatasource.getPort())).setDatabaseName(dbDatasource.getDbName()).setType(DatabaseExpandType.getDatabaseType(dbDatasource.getDbType()));
                if (StringUtils.isNotBlank(dbDatasource.getExtra())) {
                    settingDatasourceDTO(dataSourceDTO, JacksonUtil.fromJson(dbDatasource.getExtra(), Map.class), dataSourceDTO.getType());
                }
                Optional<DbDatasourceParameters> optional = dbDatasourceParametersHandler.getDatasourceParameter(dataSourceDTO.getType().getType());
                optional.ifPresent(dbDatasourceParameters -> dataSourceDTO.setParameterMap(Objects.requireNonNull(JacksonUtil.fromJson(dbDatasourceParameters.getParameters(), Map.class))));
                return dataSourceDTO;
            }
        } else {
            DatasourceDTO dataSourceDTO = new DatasourceDTO();
            dataSourceDTO.setName(dataSourceVO.getDatasourceName()).setDataSourceId(sourceId).setVersion(dataSourceVO.getVersion()).setUsername(dataSourceVO.getUsername()).setPassword(dataSourceVO.getPassword()).setHost(dataSourceVO.getHost()).setPort(Integer.valueOf(dataSourceVO.getPort())).setDatabaseName(dataSourceVO.getDatabaseName()).setType(DatabaseExpandType.getDatabaseType(dataSourceVO.getType()));
            if (!CollectionUtils.isEmpty(dataSourceVO.getExtra())) {
                settingDatasourceDTO(dataSourceDTO, dataSourceVO.getExtra(), Objects.requireNonNull(DatabaseExpandType.getDatabaseType(dataSourceVO.getType())));
            }
            Optional<DbDatasourceParameters> optional = dbDatasourceParametersHandler.getDatasourceParameter(dataSourceDTO.getType().getType());
            optional.ifPresent(dbDatasourceParameters -> dataSourceDTO.setParameterMap(Objects.requireNonNull(JacksonUtil.fromJson(dbDatasourceParameters.getParameters(), Map.class))));
            return dataSourceDTO;
        }
        return null;
    }

    private void settingDatasourceDTO(DatasourceDTO dataSourceDTO, Map<String, Object> extra, @NotNull DatabaseExpandType databaseType) {
        if (databaseType.equals(DatabaseExpandType.ORACLE)) {
            dataSourceDTO.setPdbName(Objects.nonNull(extra.get("pdbName")) ? (String) extra.get("pdbName") : "");
        }
        String clusterHosts = Objects.nonNull(extra.get("clusterNodes")) ? (String) extra.get("clusterNodes") : "";
        if (StringUtils.isNotBlank(clusterHosts)) {
            String[] clusterHostItems = StringUtils.split(clusterHosts, ',');
            for (String clusterHost : clusterHostItems) {
                DatasourceDTO clusterSource = dataSourceDTO.copyFromCurrentObject();
                clusterSource.setHost(clusterHost);
                dataSourceDTO.addClusterList(clusterSource);
            }
        }
    }

    private String createDatasource(@NotNull DatasourceVO datasourceVO) {
        DbDatasource dbDatasource = new DbDatasource();
        dbDatasource.setDatasourceName(datasourceVO.getDatasourceName());
        dbDatasource.setDbType(datasourceVO.getType());
        dbDatasource.setHost(datasourceVO.getHost());
        dbDatasource.setPort(datasourceVO.getPort());
        dbDatasource.setUserName(datasourceVO.getUsername());
        dbDatasource.setPassword(EncryptUtil.encryptPassword(datasourceVO.getPassword()));
        dbDatasource.setVersion(datasourceVO.getVersion());
        dbDatasource.setDbName(datasourceVO.getDatabaseName());
        if (!CollectionUtils.isEmpty(datasourceVO.getExtra())) {
            dbDatasource.setExtra(JacksonUtil.toJson(datasourceVO.getExtra()));
        }
        return String.valueOf(dbDatasourceHandler.save(dbDatasource));
    }

    /**
     * 初始化采集表的元数据
     *
     * @param taskId
     * @param jdbcTemplate
     * @param captureSchemaName
     * @param captureTableSettingDTO
     * @param migrateType
     * @param databaseType
     * @throws InterruptedException
     */
    private void initCaptureTableMetadata(String taskId, JdbcTemplate jdbcTemplate, String captureSchemaName, CaptureTableSettingDTO captureTableSettingDTO, String migrateType, String databaseType, @NotNull DatasourceDTO datasourceDTO) throws InterruptedException, IOException, SQLException {
        log.info("任务Id:【{}】初始化迁移表元数据......", taskId);
        List<CaptureTableDTO> excludeTables = Objects.nonNull(captureTableSettingDTO) ? captureTableSettingDTO.getExcludeTables() : null;
        List<CaptureTableDTO> includeTables = Objects.nonNull(captureTableSettingDTO) ? captureTableSettingDTO.getIncludeTables() : null;
        Map<String, CaptureTableDTO> includeTableMap = !CollectionUtils.isEmpty(includeTables) ? includeTables.stream().collect(Collectors.toMap(captureTableDTO -> MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName()), Function.identity(), (key1, key2) -> key2)) : null;
        Map<String, CaptureTableDTO> excludeTableMap = !CollectionUtils.isEmpty(excludeTables) ? excludeTables.stream().collect(Collectors.toMap(captureTableDTO -> MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName()), Function.identity(), (key1, key2) -> key2)) : null;
        DatabaseFeatures databaseFeatures = databaseFeaturesMap.get(databaseType + "_FEATURES");
        datasourceDTO.setVersion(databaseFeatures.getDatabaseVersion(jdbcTemplate));
        List<CaptureTableDTO> captureTableDTOList = databaseFeatures.searchAllCaptureTablesNoColumns(jdbcTemplate, captureSchemaName, includeTableMap, excludeTableMap, datasourceDTO.getVersion());
        if (!CollectionUtils.isEmpty(captureTableDTOList)) {
            ExecutorService executorService = new ThreadPoolExecutor(commonConfig.getCorePoolSize(), commonConfig.getCorePoolSize() * 2, 10, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(65536), new ThreadFactoryBuilder().setNameFormat(Constants.FETCH_TABLE_METADATA_THREAD_NAME).build(), new ThreadPoolExecutor.AbortPolicy());
            CountDownLatch countDownLatch = new CountDownLatch(captureTableDTOList.size());
            for (CaptureTableDTO captureTableDTO : captureTableDTOList) {
                executorService.submit(() -> {
                    try {
                        databaseFeatures.generatorCaptureTableAllColumns(taskId, jdbcTemplate, captureTableDTO, migrateType, datasourceDTO.getVersion());
                        CaptureTableCache.INSTANCE.put(MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName()), captureTableDTO);
                        CaptureTableCache.INSTANCE.put0(captureTableDTO.getSchema(), captureTableDTO);
                        log.info("获取{}.{}表的元数据......", captureTableDTO.getSchema(), captureTableDTO.getName());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        countDownLatch.countDown();
                    }

                });
            }
            countDownLatch.await();
            executorService.shutdown();
            TableMetadataCache.INSTANCE.add(captureTableDTOList);
            log.debug("需要迁移的表名称列表：\n{}", Joiner.on("\t\n").join(captureTableDTOList.stream().map(captureTableDTO -> MessageFormat.format(Constants.SCHEMA_TABLE_KEY, captureTableDTO.getSchema(), captureTableDTO.getName())).collect(Collectors.toList())));
        } else {
            log.error("任务Id:【{}】【{}】迁移任务失败!原因是采集列表为空!", taskId, MigrateType.getMigrateType(migrateType).getValue());
            terminate(taskId);
        }
    }

    /**
     * 停止任务
     *
     * @param taskId
     * @throws IOException
     */
    public void terminate(String taskId) throws IOException {
        if (Objects.nonNull(migrateCaptureService)) {
            migrateCaptureService.close();
        }
        System.exit(SpringApplication.exit(applicationContext, () -> 0));
    }
}

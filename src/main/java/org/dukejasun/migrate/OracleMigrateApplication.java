package org.dukejasun.migrate;

import lombok.extern.slf4j.Slf4j;
import org.dukejasun.migrate.common.CommonConfig;
import org.dukejasun.migrate.manager.ReplicatorManager;
import org.dukejasun.migrate.repository.impl.BaseRepositoryImpl;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.CollectionUtils;

import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;

/**
 * @author dukedpsun
 */
@Slf4j
@EnableScheduling
@SpringBootApplication
@EnableAsync(proxyTargetClass = true)
@EnableJpaRepositories(repositoryBaseClass = BaseRepositoryImpl.class)
public class OracleMigrateApplication implements ApplicationRunner {

    private final CommonConfig commonConfig;
    private final ReplicatorManager replicatorManager;
    private static final String DATA_MAPPING_LOCATION = "classpath:data_mapping/{0}/mapping.xml";

    @Autowired
    public OracleMigrateApplication(CommonConfig commonConfig, ReplicatorManager replicatorManager) {
        this.commonConfig = commonConfig;
        this.replicatorManager = replicatorManager;
    }

    public static void main(String[] args) {
        SpringApplication.run(OracleMigrateApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        new FileSystemXmlApplicationContext(Objects.nonNull(commonConfig.getDataTypeMappingLocation()) ? commonConfig.getDataTypeMappingLocation() : MessageFormat.format(DATA_MAPPING_LOCATION, commonConfig.getDataTypeMappingKey().substring(commonConfig.getDataTypeMappingKey().indexOf("2") + 1).toLowerCase()));
        settingStartParameter(args);
    }

    /**
     * 通过启动参数设置
     *
     * @param applicationArguments
     */
    private void settingStartParameter(@NotNull ApplicationArguments applicationArguments) {
        List<String> replicateTypeList = applicationArguments.getOptionValues("replicateType");
        if (!CollectionUtils.isEmpty(replicateTypeList)) {
            String incrementReplicateType = replicateTypeList.get(0);
            commonConfig.setReplicateType(Integer.valueOf(incrementReplicateType));
        }
        List<String> totalFileThreshold = applicationArguments.getOptionValues("dataFileThreshold");
        if (!CollectionUtils.isEmpty(totalFileThreshold)) {
            commonConfig.setDataFileThreshold(totalFileThreshold.get(0));
        }
        List<String> totalTaskIdList = applicationArguments.getOptionValues("totalTaskId");
        if (!CollectionUtils.isEmpty(totalTaskIdList)) {
            String totalTaskId = totalTaskIdList.get(0);
            commonConfig.setTotalTaskId(totalTaskId);
        }
    }
}


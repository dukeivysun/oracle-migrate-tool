package org.dukejasun.migrate.parse;

import org.dukejasun.migrate.cache.local.DataTypeMappingCache;
import org.dukejasun.migrate.model.dto.mapping.DataTypeMappingDTO;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * @author dukedpsun
 */
public class MappingBeanDefinitionParser implements BeanDefinitionParser {

    public MappingBeanDefinitionParser() {
    }

    @Override
    public BeanDefinition parse(@NotNull Element element, @NotNull ParserContext parserContext) {
        String source = element.getAttribute("source");
        String target = element.getAttribute("target");
        String sourceVersion = element.getAttribute("sourceVersion");
        String targetVersion = element.getAttribute("targetVersion");
        String sourceName = element.getAttribute("sourceName");
        String targetName = element.getAttribute("targetName");
        String range = element.getAttribute("range");
        String needLength = element.getAttribute("needLength");
        String needScale = element.getAttribute("needScale");
        String replaceType = element.getAttribute("replaceType");

        DataTypeMappingDTO dataTypeMappingDTO = new DataTypeMappingDTO();
        dataTypeMappingDTO.setSource(source);
        dataTypeMappingDTO.setTarget(target);
        dataTypeMappingDTO.setSourceVersion(sourceVersion);
        dataTypeMappingDTO.setTargetVersion(targetVersion);
        dataTypeMappingDTO.setSourceName(sourceName);
        dataTypeMappingDTO.setTargetName(targetName);
        dataTypeMappingDTO.setRange(range);
        dataTypeMappingDTO.setNeedLength(needLength);
        dataTypeMappingDTO.setNeedScale(needScale);
        dataTypeMappingDTO.setReplaceType(replaceType);
        DataTypeMappingCache.INSTANCE.put(source + '2' + target, dataTypeMappingDTO);
        return null;
    }
}

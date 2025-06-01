package org.dukejasun.migrate.handler;

import org.dukejasun.migrate.parse.MappingBeanDefinitionParser;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * @author dukedpsun
 */
public class DataTypeMappingHandler extends NamespaceHandlerSupport {

    @Override
    public void init() {
        registerBeanDefinitionParser("mapping", new MappingBeanDefinitionParser());
    }
}

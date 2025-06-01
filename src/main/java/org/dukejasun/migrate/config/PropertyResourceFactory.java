package org.dukejasun.migrate.config;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author dukedpsun
 */
public class PropertyResourceFactory implements PropertySourceFactory {
    private static final String[] ENDS_WITH = {".yml", ".yaml"};

    @Override
    public @NotNull PropertySource<?> createPropertySource(@Nullable String name, @NotNull EncodedResource encodedResource) throws IOException {
        String resourceName = Optional.ofNullable(name).orElse(encodedResource.getResource().getFilename());
        assert resourceName != null;
        if (resourceName.endsWith(ENDS_WITH[0]) || resourceName.endsWith(ENDS_WITH[1])) {
            List<PropertySource<?>> yamlSources = new YamlPropertySourceLoader().load(resourceName, encodedResource.getResource());
            return yamlSources.get(0);
        } else {
            return new PropertiesPropertySource(resourceName, new Properties());
        }
    }
}

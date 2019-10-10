/*
 * Copyright (C) 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.cloud.nacos.client;

import com.alibaba.cloud.nacos.NacosPropertySourceRepository;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.*;

/**
 * @author xiaojing
 * @author pbting
 */
public class NacosPropertySourceBuilder {
    private static final Logger log = LoggerFactory
            .getLogger(NacosPropertySourceBuilder.class);
    private static final Properties EMPTY_PROPERTIES = new Properties();
    private static final Map<String, Object> EMPTY_MAP = new LinkedHashMap<>();

    private ConfigService configService;
    private long timeout;

    public NacosPropertySourceBuilder(ConfigService configService, long timeout) {
        this.configService = configService;
        this.timeout = timeout;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public ConfigService getConfigService() {
        return configService;
    }

    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    /**
     * @param dataId Nacos dataId
     * @param group  Nacos group
     */
    NacosPropertySource build(String dataId, String group, String fileExtension,
                              boolean isRefreshable) {
//		Properties p = loadNacosData(dataId, group, fileExtension);
        Map<String, Object> p = loadNacosDataMap(dataId, group, fileExtension);
        NacosPropertySource nacosPropertySource = new NacosPropertySource(group, dataId, p, new Date(), isRefreshable);
        NacosPropertySourceRepository.collectNacosPropertySources(nacosPropertySource);
        return nacosPropertySource;
    }

    private Map<String, Object> loadNacosDataMap(String dataId, String group, String fileExtension) {
        String data = null;
        try {
            data = configService.getConfig(dataId, group, timeout);

            if (!StringUtils.isEmpty(data)) {
                log.info(String.format("Loading nacos data, dataId: '%s', group: '%s'",
                        dataId, group));

                if ("properties".equalsIgnoreCase(fileExtension)) {
                    Properties properties = new OrderedProperties();
                    properties.load(new StringReader(data));

                    StringReader reader = new StringReader(data);
                    BufferedReader br = new BufferedReader(reader);
                    String line = br.readLine();
                    String[] arr;
                    while (line != null) {

                        arr = line.split("=");
                        if (arr.length != 2) {
                            throw new Exception("error properties format");
                        }

                        properties.put(arr[0], arr[1]);
                        line = br.readLine();
                    }

                    return propertiesToMap(properties);
                } else if ("yaml".equalsIgnoreCase(fileExtension)
                        || "yml".equalsIgnoreCase(fileExtension)) {

                    YamlMapFactoryBean y = new YamlMapFactoryBean();
                    y.setResources(new ByteArrayResource(data.getBytes()));
                    Map<String, Object> result = new LinkedHashMap<>();
                    flattenedMap(result, y.getObject(), null);

                    return result;
                }
            }

        } catch (NacosException e) {
            log.error("get data from Nacos error,dataId:{}, ", dataId, e);
        } catch (Exception e) {
            log.error("parse data from Nacos error,dataId:{},data:{},", dataId, data, e);
        }
        return EMPTY_MAP;
    }

    @Deprecated
    private Properties loadNacosData(String dataId, String group, String fileExtension) {
        String data = null;
        try {
            data = configService.getConfig(dataId, group, timeout);
            if (!StringUtils.isEmpty(data)) {
                log.info(String.format("Loading nacos data, dataId: '%s', group: '%s'",
                        dataId, group));

                if ("properties".equalsIgnoreCase(fileExtension)) {
                    Properties properties = new Properties();
                    properties.load(new StringReader(data));
                    return properties;
                } else if ("yaml".equalsIgnoreCase(fileExtension)
                        || "yml".equalsIgnoreCase(fileExtension)) {
                    YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
                    yamlFactory.setResources(new ByteArrayResource(data.getBytes()));
                    return yamlFactory.getObject();
                }

            }
        } catch (NacosException e) {
            log.error("get data from Nacos error,dataId:{}, ", dataId, e);
        } catch (Exception e) {
            log.error("parse data from Nacos error,dataId:{},data:{},", dataId, data, e);
        }
        return EMPTY_PROPERTIES;

    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> propertiesToMap(Properties properties) {
        Map<String, Object> result = new LinkedHashMap<>(16);
        Set<String> keys = properties.stringPropertyNames();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Object value = properties.getProperty(key);
            if (value != null) {
                result.put(key, ((String) value).trim());
            } else {
                result.put(key, null);
            }
        }
        return result;
    }

    private void flattenedMap(Map<String, Object> result, @Nullable Map<String, Object> source, @Nullable String path) {
        if (source == null) {
            return;
        }
        source.forEach((key, value) -> {
            if (StringUtils.hasText(path)) {
                if (key.startsWith("[")) {
                    key = path + key;
                } else {
                    key = path + '.' + key;
                }
            }
            if (value instanceof String) {
                result.put(key, value);
            } else if (value instanceof Map) {
                // Need a compound key
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;
                flattenedMap(result, map, key);
            } else if (value instanceof Collection) {
                // Need a compound key
                @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) value;
                if (collection.isEmpty()) {
                    result.put(key, "");
                } else {
                    int count = 0;
                    for (Object object : collection) {
                        flattenedMap(result, Collections.singletonMap(
                                "[" + (count++) + "]", object), key);
                    }
                }
            } else {
                result.put(key, (value != null ? value : ""));
            }
        });
    }

    /**
     * 内部构造的顺序Properties类
     */
    private static class OrderedProperties extends Properties {

        private final LinkedHashSet<Object> keys = new LinkedHashSet();

        @Override
        public Enumeration<Object> keys() {
            return Collections.<Object>enumeration(keys);
        }

        @Override
        public Object put(Object key, Object value) {
            keys.add(key);
            return super.put(key, value);
        }

        @Override
        public Set<Object> keySet() {
            return keys;
        }

        @Override
        public Set<String> stringPropertyNames() {
            Set<String> set = new LinkedHashSet();

            for (Object key : this.keys) {
                set.add((String) key);
            }

            return set;
        }
    }

}

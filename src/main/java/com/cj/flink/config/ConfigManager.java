package com.cj.flink.config;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * @author Lee
 */
public class ConfigManager {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = ConfigManager.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(new InputStreamReader(in, "UTF-8"));
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static void setPropertys(Map<String, String> maps) {
        for (Map.Entry<String, String> entry : maps.entrySet()) {
            prop.setProperty(entry.getKey(), entry.getValue());
        }
    }
}

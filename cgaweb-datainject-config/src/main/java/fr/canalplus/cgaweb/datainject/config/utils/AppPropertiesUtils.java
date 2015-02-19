package fr.canalplus.cgaweb.datainject.config.utils;

import java.util.Properties;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

@Service
public class AppPropertiesUtils {

	@Resource(name = "app-properties")
	private Properties appProperties;

	public String getValueByKey(String key) {
		String value = appProperties.getProperty(key);
		return value;
	}
}

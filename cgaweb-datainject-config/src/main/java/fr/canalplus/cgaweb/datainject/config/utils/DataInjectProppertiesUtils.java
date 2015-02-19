package fr.canalplus.cgaweb.datainject.config.utils;

import java.util.Properties;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

@Service
public class DataInjectProppertiesUtils {

	@Resource(name = "datainject-properties")
	private Properties datainjectProperties;

	public String getValueByKey(String key) {
		String value = datainjectProperties.getProperty(key);
		return value;
	}
}

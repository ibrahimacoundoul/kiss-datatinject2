package fr.canalplus.cgaweb.datainject.variabilisation.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections.MapUtils;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.springframework.stereotype.Service;

import fr.canalplus.cgaweb.datainject.config.DataInjectConfig;
import fr.canalplus.cgaweb.datainject.config.utils.AppPropertiesUtils;
import fr.canalplus.cgaweb.datainject.variabilisation.IDataInjectnVarialibisation;

@Service
public class DataInjectVarialibisation implements IDataInjectnVarialibisation {

	static Map<String, String> keyValueParam = new HashMap<String, String>();
	
	@Inject
	private AppPropertiesUtils appPropertiesUtils;
	
	public Map<String, String> getVariabilizedColumns(long rootTablepkValue) throws Exception {

		createVariableXmlFileForTable(rootTablepkValue);

		return keyValueParam;
	}

	private void initMap() {
		keyValueParam.put("[NOM]", "COUNDOUL 5 OUAFI");
		keyValueParam.put("[PRENOM]", "Ibrahima Khalid");
	}

	private void createVariableXmlFileForTable(long rootTablepkValue) throws Exception {

		System.out.println("\nBegin : Variabilization in progress...");
		
		initMap();

		StringBuffer variabilizedFile = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir")).append("Variable_")
				.append(DataInjectConfig.DEPENDANT_DATA_FILE_NAME).append(DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + rootTablepkValue)
				.append(DataInjectConfig.XML_FILE_EXTENTION);

		if (DataInjectConfig.ABO_ABONN_TABLE_NAME != null && variabilizedFile != null && MapUtils.isNotEmpty(keyValueParam)) {

			try {

				String dependentDataFile = new StringBuffer(appPropertiesUtils.getValueByKey("outputDir")).append(DataInjectConfig.DEPENDANT_DATA_FILE_NAME)
						.append(DataInjectConfig.ABO_ABONN_TABLE_NAME + "_" + rootTablepkValue).append(DataInjectConfig.XML_FILE_EXTENTION).toString();

				File fromXmlFile = new File(dependentDataFile);
				SAXBuilder builder = new SAXBuilder();
				Document doc = (Document) builder.build(fromXmlFile);
				Element rootNode = doc.getRootElement();

				// update tableName id attribute
				Element tableNameElement = rootNode.getChild(DataInjectConfig.ABO_ABONN_TABLE_NAME);

				if (tableNameElement != null) {
					Set<String> keys = keyValueParam.keySet();
					for (String currentKey : keys) {
						String columnName = currentKey.replaceAll("\\[", "").replaceAll("\\]", "");
						Attribute columnAttribute = tableNameElement.getAttribute(columnName);
						if (columnAttribute != null) {
							columnAttribute.setValue(currentKey);
						}
					}
					XMLOutputter xmlOutput = new XMLOutputter();
					xmlOutput.setFormat(Format.getPrettyFormat());
					xmlOutput.output(doc, new FileWriter(variabilizedFile.toString()));

					System.out.println("variabilization: OK");
				}
			} catch (JDOMException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("\nEnd : Variabilization.");
		}
	}
}

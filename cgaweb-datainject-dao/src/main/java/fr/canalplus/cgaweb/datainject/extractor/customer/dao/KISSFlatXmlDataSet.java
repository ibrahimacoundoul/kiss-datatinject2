package fr.canalplus.cgaweb.datainject.extractor.customer.dao;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;

import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatDtdDataSet;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

public class KISSFlatXmlDataSet extends CachedDataSet {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(KISSFlatXmlDataSet.class);

	/**
	 * Creates a new {@link KISSFlatXmlDataSet} with the data of the given producer.
	 * @param flatXmlProducer The producer that provides the {@link KISSFlatXmlDataSet} content
	 * @throws DataSetException 
	 * @since 2.4.7
	 */
	public KISSFlatXmlDataSet(FlatXmlProducer flatXmlProducer) throws DataSetException {
		super(flatXmlProducer, flatXmlProducer.isCaseSensitiveTableNames());
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified InputSource.
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(InputSource source) throws IOException, DataSetException {
		super(new FlatXmlProducer(source));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlFile the xml file
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(File xmlFile) throws IOException, DataSetException {
		this(xmlFile, true);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlFile the xml file
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(File xmlFile, boolean dtdMetadata) throws IOException, DataSetException {
		this(xmlFile.toURL(), dtdMetadata);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlFile the xml file
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @param columnSensing Whether or not the columns should be sensed automatically. Every XML row
	 * is scanned for columns that have not been there in a previous column.
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(File xmlFile, boolean dtdMetadata, boolean columnSensing) throws IOException, DataSetException {
		this(xmlFile.toURL(), dtdMetadata, columnSensing);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlFile the xml file
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @param columnSensing Whether or not the columns should be sensed automatically. Every XML row
	 * is scanned for columns that have not been there in a previous column.
	 * @param caseSensitiveTableNames Whether or not this dataset should use case sensitive table names
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(File xmlFile, boolean dtdMetadata, boolean columnSensing, boolean caseSensitiveTableNames) throws IOException,
			DataSetException {
		this(xmlFile.toURL(), dtdMetadata, columnSensing, caseSensitiveTableNames);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml URL.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlUrl the xml URL
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(URL xmlUrl) throws IOException, DataSetException {
		this(xmlUrl, true);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml URL.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlUrl the xml URL
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(URL xmlUrl, boolean dtdMetadata) throws IOException, DataSetException {
		this(xmlUrl, dtdMetadata, false);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml URL.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlUrl the xml URL
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @param columnSensing Whether or not the columns should be sensed automatically. Every XML row
	 * is scanned for columns that have not been there in a previous column.
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(URL xmlUrl, boolean dtdMetadata, boolean columnSensing) throws IOException, DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlUrl.toString()), dtdMetadata, columnSensing));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlUrl the xml file
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @param columnSensing Whether or not the columns should be sensed automatically. Every XML row
	 * is scanned for columns that have not been there in a previous column.
	 * @param caseSensitiveTableNames Whether or not this dataset should use case sensitive table names
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(URL xmlUrl, boolean dtdMetadata, boolean columnSensing, boolean caseSensitiveTableNames) throws IOException,
			DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlUrl.toString()), dtdMetadata, columnSensing, caseSensitiveTableNames), caseSensitiveTableNames);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml reader.
	 * Relative DOCTYPE uri are resolved from the current working directory.
	 *
	 * @param xmlReader the xml reader
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(Reader xmlReader) throws IOException, DataSetException {
		this(xmlReader, true);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml reader.
	 * Relative DOCTYPE uri are resolved from the current working directory.
	 *
	 * @param xmlReader the xml reader
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(Reader xmlReader, boolean dtdMetadata) throws IOException, DataSetException {
		this(xmlReader, dtdMetadata, false, false);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml file.
	 * Relative DOCTYPE uri are resolved from the xml file path.
	 *
	 * @param xmlReader the xml reader
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @param columnSensing Whether or not the columns should be sensed automatically. Every XML row
	 * is scanned for columns that have not been there in a previous column.
	 * @param caseSensitiveTableNames Whether or not this dataset should use case sensitive table names
	 * @since 2.4.3
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(Reader xmlReader, boolean dtdMetadata, boolean columnSensing, boolean caseSensitiveTableNames) throws IOException,
			DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlReader), dtdMetadata, columnSensing, caseSensitiveTableNames), caseSensitiveTableNames);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml and dtd readers.
	 *
	 * @param xmlReader the xml reader
	 * @param dtdReader the dtd reader
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(Reader xmlReader, Reader dtdReader) throws IOException, DataSetException {
		this(xmlReader, new FlatDtdDataSet(dtdReader));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml reader.
	 *
	 * @param xmlReader the xml reader
	 * @param metaDataSet the dataset used as metadata source.
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(Reader xmlReader, IDataSet metaDataSet) throws IOException, DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlReader), metaDataSet));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml input stream.
	 * Relative DOCTYPE uri are resolved from the current working directory.
	 *
	 * @param xmlStream the xml input stream
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(InputStream xmlStream) throws IOException, DataSetException {
		this(xmlStream, true);
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml input stream.
	 * Relative DOCTYPE uri are resolved from the current working directory.
	 *
	 * @param xmlStream the xml input stream
	 * @param dtdMetadata if <code>false</code> do not use DTD as metadata
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(InputStream xmlStream, boolean dtdMetadata) throws IOException, DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlStream), dtdMetadata));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml and dtd input
	 * stream.
	 *
	 * @param xmlStream the xml input stream
	 * @param dtdStream the dtd input stream
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(InputStream xmlStream, InputStream dtdStream) throws IOException, DataSetException {
		this(xmlStream, new FlatDtdDataSet(dtdStream));
	}

	/**
	 * Creates an KISSFlatXmlDataSet object with the specified xml input stream.
	 *
	 * @param xmlStream the xml input stream
	 * @param metaDataSet the dataset used as metadata source.
	 * @deprecated since 2.4.7 - use {@link KISSFlatXmlDataSetBuilder} to create a {@link KISSFlatXmlDataSet}
	 */
	public KISSFlatXmlDataSet(InputStream xmlStream, IDataSet metaDataSet) throws IOException, DataSetException {
		super(new FlatXmlProducer(new InputSource(xmlStream), metaDataSet));
	}

	/**
	 * Write the specified dataset to the specified output stream as xml.
	 */
	public static void write(IDataSet dataSet, OutputStream out) throws IOException, DataSetException {
		logger.debug("write(dataSet={}, out={}) - start", dataSet, out);

		KISSFlatXmlWriter datasetWriter = new KISSFlatXmlWriter(out);
		datasetWriter.setIncludeEmptyTable(true);
		datasetWriter.write(dataSet);
	}

	/**
	 * Write the specified dataset to the specified writer as xml.
	 */
	public static void write(IDataSet dataSet, Writer writer) throws IOException, DataSetException {
		logger.debug("write(dataSet={}, writer={}) - start", dataSet, writer);
		write(dataSet, writer, null);
	}

	/**
	 * Write the specified dataset to the specified writer as xml.
	 */
	public static void write(IDataSet dataSet, Writer writer, String encoding) throws IOException, DataSetException {
		if (logger.isDebugEnabled()) {
			logger.debug("write(dataSet={}, writer={}, encoding={}) - start", new Object[] { dataSet, writer, encoding });
		}

		FlatXmlWriter datasetWriter = new FlatXmlWriter(writer, encoding);
		datasetWriter.setIncludeEmptyTable(true);
		datasetWriter.write(dataSet);
	}

	/**
	 * Write a DTD for the specified dataset to the specified output.
	 * @deprecated use {@link FlatDtdDataSet#write}
	 */
	public static void writeDtd(IDataSet dataSet, OutputStream out) throws IOException, DataSetException {
		logger.debug("writeDtd(dataSet={}, out={}) - start", dataSet, out);
		FlatDtdDataSet.write(dataSet, out);
	}
}
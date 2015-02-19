package fr.canalplus.cgaweb.datainject.extractor.customer.dao;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KISSDataSetProducerAdapter implements IDataSetProducer {
	/**
	* Logger for this class
	*/
	private static final Logger logger = LoggerFactory.getLogger(KISSDataSetProducerAdapter.class);

	private static final IDataSetConsumer EMPTY_CONSUMER = new DefaultConsumer();

	private final ITableIterator _iterator;
	private IDataSetConsumer _consumer = EMPTY_CONSUMER;

	public KISSDataSetProducerAdapter(ITableIterator iterator) {
		_iterator = iterator;
	}

	public KISSDataSetProducerAdapter(IDataSet dataSet) throws DataSetException {
		_iterator = dataSet.iterator();
	}

	////////////////////////////////////////////////////////////////////////////
	// IDataSetProducer interface

	public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
		logger.debug("setConsumer(consumer) - start");

		_consumer = consumer;
	}

	public void produce() throws DataSetException {
		logger.debug("produce() - start");

		_consumer.startDataSet();
		while (_iterator.next()) {
			ITable table = _iterator.getTable();
			ITableMetaData metaData = table.getTableMetaData();

			_consumer.startTable(metaData);
			try {
				Column[] columns = metaData.getColumns();
				if (columns.length == 0) {
					_consumer.endTable();
					continue;
				}

				for (int i = 0;; i++) {
					Object[] values = new Object[columns.length];
					for (int j = 0; j < columns.length; j++) {
						Column column = columns[j];
						values[j] = table.getValue(i, column.getColumnName());
					}
					_consumer.row(values);
				}
			} catch (RowOutOfBoundsException e) {
				// This exception occurs when records are exhausted
				// and we reach the end of the table.  Ignore this error
				// and close table.

				// end of table
				_consumer.endTable();
			}
		}
		_consumer.endDataSet();
	}
}
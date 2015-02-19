package fr.canalplus.cgaweb.datainject.exceptions;

public interface IQualifiedException {

	public static final Object[] EMPTY_ARRAY = {};

	String getErrorCode();

	Object[] getMessageArgs();
}

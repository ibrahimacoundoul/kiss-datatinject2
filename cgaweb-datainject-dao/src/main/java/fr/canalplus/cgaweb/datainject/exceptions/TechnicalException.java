package fr.canalplus.cgaweb.datainject.exceptions;

import java.util.Arrays;

public class TechnicalException extends RuntimeException implements IQualifiedException {

	private static final long serialVersionUID = 1L;

	private final ErrorCode errorCode;

	private final Object[] messageArgs;

	public static enum ErrorCode {
		ERR_TECH_MISSING_DATA,
		ERR_TECH_GEN_DEFAULT,
		ERR_TECH_INVALID_NUMABO,
		ERR_TECH_AMBIGUOUS_TABLE_NANE,
		ERR_TECH_SERACH_EXCEPTION,
		ERR_TECH_DATA_SET,
		ERR_TECH_FILE_NOT_FOUND,
		ERR_TECH_IO,
		ERR_TECH_GETTING_CONNECTION,
		ERR_TECH_DATABASE_UNIT,
		ERR_TECH_SQL_EXCEPTION,
		ERR_TECH_MALFORMED_URL,
	}

	public TechnicalException(ErrorCode errorCode) {
		super(errorCode.name());
		this.errorCode = errorCode;
		this.messageArgs = EMPTY_ARRAY;
	}

	public TechnicalException(ErrorCode errorCode, Object... messageArgs) {
		super(errorCode.name());
		this.errorCode = errorCode;
		this.messageArgs = messageArgs;
	}

	public TechnicalException(ErrorCode errorCode, Throwable cause) {
		super(errorCode.name(), cause);
		this.errorCode = errorCode;
		this.messageArgs = EMPTY_ARRAY;
	}

	public TechnicalException(ErrorCode errorCode, Throwable cause, Object... messageArgs) {
		super(errorCode.name(), cause);
		this.errorCode = errorCode;
		this.messageArgs = messageArgs;
	}

	public String getErrorCode() {
		return errorCode.name();
	}

	public Object[] getMessageArgs() {
		Object[] localMessageArgs = Arrays.copyOf(this.messageArgs, this.messageArgs.length);
		return localMessageArgs;
	}
}

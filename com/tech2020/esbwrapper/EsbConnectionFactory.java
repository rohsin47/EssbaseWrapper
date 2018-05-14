package com.tech2020.esbwrapper;

public class EsbConnectionFactory {
	private EsbConnection connection;
	private static final EsbConnectionFactory conFactory = new EsbConnectionFactory();

	public static EsbConnectionFactory getConnectionFactory() {
		return conFactory;
	}

	public EsbConnection getConnection() {
		return this.connection;
	}

	public void setConnection() {
		if (this.connection == null) {
			this.connection = new EsbConnection();
		}
	}
}

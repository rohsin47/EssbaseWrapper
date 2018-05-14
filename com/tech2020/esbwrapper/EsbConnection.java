package com.tech2020.esbwrapper;

import com.essbase.api.base.EssException;
import com.essbase.api.datasource.IEssOlapServer;
import com.essbase.api.domain.IEssDomain;
import com.essbase.api.session.Essbase;
import com.essbase.api.session.IEssbase;
import com.essbase.api.session.IEssbase.Home;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EsbConnection {
	private static final Log log = LogFactory.getLog(EsbConnection.class);
	private String esbUserName;
	private String esbpassword;
	private String esbprovider;
	private String esbserverName;
	private Essbase ess = null;
	private IEssOlapServer olapSvr = null;

	public EsbConnection() {
		createJAPI();
		initConnectionParams();
		setOlapServer();
	}

	public void connectToOLAP(IEssOlapServer olapServer) {
		try {
			if (olapServer != null) {
				this.olapSvr.connect();
			}
			log.info("Connected to OLAP Server");
		} catch (EssException e) {
			EmailUtil.sendErrorMail("An error occured in connecting to OLAP Server", e);
			log.error("An error occured in connecting to OLAP Server.", e);
		}
	}

	public void disconnectFromOLAP(IEssOlapServer olapServer) {
		try {
			if (this.ess.isSignedOn()) {
				if (olapServer.isConnected()) {
					olapServer.disconnect();
				}
				this.ess.signOff();
			}
			this.ess = null;
			this.olapSvr = null;
			log.info("Disconnected from OLAP Server");
		} catch (EssException e) {
			EmailUtil.sendErrorMail("An error occured in disconnecting from OLAP Server", e);
			log.error("An error occured in disconnecting from OLAP Server.", e);
		}
	}

	public boolean isOLAPConnected(IEssOlapServer olapServer) {
		boolean isConnected = false;
		try {
			isConnected = olapServer.isConnected();
		} catch (EssException e) {
			isConnected = false;
		}
		return isConnected;
	}

	public IEssOlapServer getOLAPServer() {
		try {
			if (!this.ess.isSignedOn()) {
				setOlapServer();
			}
		} catch (EssException e) {
			log.error("An error occured in getting Olap Server.", e);
		}
		return this.olapSvr;
	}

	public void clearActive(IEssOlapServer olapSvr) {
		try {
			if (olapSvr.getActive() != null) {
				olapSvr.clearActive();
			}
		} catch (EssException e) {
			log.error("An error occured in clearing active on cube.", e);
		}
	}

	public IEssbase getEssbaseInstance() {
		return this.ess;
	}

	public String getEsbUserName() {
		return this.esbUserName;
	}

	public void setEsbUserName(String esbUserName) {
		this.esbUserName = esbUserName;
	}

	public String getEsbpassword() {
		return this.esbpassword;
	}

	public void setEsbpassword(String esbpassword) {
		this.esbpassword = esbpassword;
	}

	public String getEsbprovider() {
		return this.esbprovider;
	}

	public void setEsbprovider(String esbprovider) {
		this.esbprovider = esbprovider;
	}

	public String getEsbserverName() {
		return this.esbserverName;
	}

	public void setEsbserverName(String esbserverName) {
		this.esbserverName = esbserverName;
	}

	private void createJAPI() {
		try {
			if (this.ess == null) {
				this.ess = ((Essbase) IEssbase.Home.create("11.1.2.3"));
			}
		} catch (EssException e) {
			EmailUtil.sendErrorMail("An error occured in initialization of Essbase", e);
			log.error("An error occured in initialization of Essbase.", e);
		}
	}

	private void initConnectionParams() {
		setEsbUserName("");
		setEsbpassword("");
		setEsbprovider("");
		setEsbserverName("");
	}

	private void setOlapServer() {
		try {
			if (this.ess.getApiVersion().equals("11.1.2.3")) {
				this.ess.signOn(this.esbUserName, this.esbpassword, false, null, this.esbprovider);
				this.olapSvr = this.ess.getRootDomain().getOlapServer(this.esbserverName);
			}
		} catch (EssException e) {
			EmailUtil.sendErrorMail("An error occured while signing on to Essbase", e);
			log.error("An error occured while signing on to Essbase.", e);
		}
	}
}

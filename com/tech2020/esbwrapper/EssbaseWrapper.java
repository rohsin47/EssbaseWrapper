package com.tech2020.esbwrapper;

import com.essbase.api.base.EssException;
import com.essbase.api.base.IEssBaseObject;
import com.essbase.api.base.IEssIterator;
import com.essbase.api.datasource.EssPartitionInfo;
import com.essbase.api.datasource.IEssCube;
import com.essbase.api.datasource.IEssCubeDataloadInstance;
import com.essbase.api.datasource.IEssMaxlSession;
import com.essbase.api.datasource.IEssOlapApplication;
import com.essbase.api.datasource.IEssOlapRequest;
import com.essbase.api.datasource.IEssOlapServer;
import com.essbase.api.datasource.IEssOlapServer.IEssOlapConnectionInfo;
import com.essbase.api.metadata.IEssCubeOutline;
import com.essbase.api.session.IEssbase;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EssbaseWrapper {
	private static final Log LOGGER = LogFactory.getLog(EssbaseWrapper.class);
	private static String esbUserName;
	private static String esbpassword;
	private static String esbprovider;
	private static String esbserverName;
	private static IEssbase ess = null;
	private static IEssOlapServer olapSvr = null;
	private static EsbConnection esbConnection = null;

	public EssbaseWrapper() {
		EsbConnectionFactory connectionFactory = EsbConnectionFactory.getConnectionFactory();
		connectionFactory.setConnection();
		esbConnection = connectionFactory.getConnection();
	}

	protected boolean connectToOlap() {
		Boolean isSuccess = Boolean.valueOf(false);
		if (esbConnection != null) {
			esbUserName = esbConnection.getEsbUserName();
			esbpassword = esbConnection.getEsbpassword();
			esbprovider = esbConnection.getEsbprovider();
			esbserverName = esbConnection.getEsbserverName();
			ess = esbConnection.getEssbaseInstance();
			olapSvr = esbConnection.getOLAPServer();
			if (olapSvr != null) {
				esbConnection.connectToOLAP(olapSvr);
				isSuccess = Boolean.valueOf(true);
			}
		}
		return isSuccess.booleanValue();
	}

	protected boolean disconnectFromOlap() {
		Boolean isSuccess = Boolean.valueOf(false);
		if (olapSvr != null) {
			esbConnection.disconnectFromOLAP(olapSvr);
			isSuccess = Boolean.valueOf(true);
		}
		return isSuccess.booleanValue();
	}

	protected static IEssOlapServer getOlapServer() {
		if (!esbConnection.isOLAPConnected(olapSvr)) {
			esbConnection.connectToOLAP(olapSvr);
		}
		return olapSvr;
	}

	protected static boolean closeOutline(IEssCubeOutline outline) {
		try {
			if ((outline != null) && (outline.isOpen())) {
				outline.close();
			}
		} catch (EssException e) {
			LOGGER.error("An error occured while closing Essbase Outline", e);
			return false;
		}
		return true;
	}

	protected static boolean logOffEssUsers(String appName, String cubeName) {
		boolean success = true;
		IEssOlapServer.IEssOlapConnectionInfo connInfo = null;
		IEssIterator connIter = null;
		List<String> activeUsers = null;
		try {
			connIter = olapSvr.getConnections();
			if ((connIter != null) && (connIter.getAll().length > 0)) {
				activeUsers = new ArrayList();
				for (Object baseObj : connIter.getAll()) {
					connInfo = (IEssOlapServer.IEssOlapConnectionInfo) baseObj;
					if (appName.trim().equalsIgnoreCase(connInfo.getConnectedApplicationName().trim())) {
						activeUsers.add(connInfo.getConnectedUserName());
					}
				}
				success = killOlapRequests(activeUsers, appName, cubeName);
			} else {
				LOGGER.info("No active connections to " + appName + ":)");
			}
			connIter = olapSvr.getConnections();
			if ((connIter != null) && (connIter.getAll().length > 0)) {
				for (Object baseObj : connIter.getAll()) {
					connInfo = (IEssOlapServer.IEssOlapConnectionInfo) baseObj;
					if (appName.trim().equalsIgnoreCase(connInfo.getConnectedApplicationName().trim())) {
						connInfo.logoffUser();
						LOGGER.info("Logged off " + connInfo.getConnectedUserName());
					}
				}
			} else {
				LOGGER.info("No active connections to " + appName + ":)");
			}
		} catch (EssException e) {
			LOGGER.error("An exception occured in logging off users from " + appName, e);
			success = false;
			try {
				updateEsbAppConnection(olapSvr, appName, true);
			} catch (EssException es) {
				LOGGER.error("Error in updating connection:", es);
			}
		}
		return success;
	}

	private static boolean killOlapRequests(List<String> users, String app, String cube) {
		boolean isSuccess = true;
		try {
			if (users.size() > 0) {
				for (String user : users) {
					IEssIterator requests = olapSvr.getRequests(user, app, cube);
					if (requests.getAll().length > 0) {
						for (int i = 0; i < requests.getCount(); i++) {
							IEssOlapRequest request = (IEssOlapRequest) requests.getAt(i);
							LOGGER.info("Request state : " + request.getRequestState());
							LOGGER.info("Request by " + user + " terminating");
							request.kill();
							try {
								Thread.sleep(3000L);
							} catch (InterruptedException e) {
								LOGGER.info("Request termination is interrupted..");
							}
							LOGGER.info("Request state : " + request.getRequestState());
							if (request.getRequestState() != 2) {
								isSuccess = false;
								LOGGER.info("Request by " + user
										+ " failed to terminate... Exiting.. Kindly restart after do checks.");
								break label318;
							}
							LOGGER.info("Request by " + user + " terminated");
						}
					} else {
						isSuccess = true;
					}
				}
			}
		} catch (EssException e) {
			label318: isSuccess = false;
			LOGGER.info("An exception occured in killing olap requests");
		}
		return isSuccess;
	}

	public boolean appExist(String appName) {
		boolean retVal = false;
		try {
			LOGGER.info("Looking for app:" + appName);
			for (IEssBaseObject objArr : olapSvr.getApplications().getAll()) {
				if (objArr.toString().equalsIgnoreCase(appName)) {
					retVal = true;
					break;
				}
			}
		} catch (EssException e) {
			LOGGER.error("Error testing app existence:", e);
		}
		LOGGER.info("Application " + appName + " exists:" + retVal);
		return retVal;
	}

	protected static boolean updateEsbAppConnection(IEssOlapServer olapSvr, String appName, boolean allowConnects)
			throws EssException {
		LOGGER.info("Setting Application allowConnects to " + allowConnects + " for application:" + appName);
		IEssOlapApplication app = olapSvr.getApplication(appName);
		app.setAllowConnects(allowConnects);
		app.updatePropertyValues();
		return true;
	}

	protected static boolean updateEsbAppConnection(String appName, boolean allowConnects) {
		boolean isSuccess = true;
		if (olapSvr == null) {
			return false;
		}
		try {
			updateEsbAppConnection(olapSvr, appName, allowConnects);
		} catch (EssException e) {
			LOGGER.error("An error occured while updating connections for " + appName, e);
			return false;
		}
		return isSuccess;
	}

	protected static void restructureBsoCube(String app, String cube) {
		String[] restruct = { "alter database " + app + "." + cube + " force restructure" };
		executeMaxL(restruct);
	}

	protected static boolean stopEssCube(String appName, String cubeName) {
		boolean isSuccess = true;
		if (olapSvr == null) {
			return false;
		}
		try {
			IEssCube cube = olapSvr.getApplication(appName).getCube(cubeName);
			cube.stop();
		} catch (EssException e) {
			LOGGER.error("An error occured while stopping cube for " + cubeName, e);
			return false;
		}
		return isSuccess;
	}

	protected static boolean startEssCube(String appName, String cubeName) {
		boolean isSuccess = true;
		if (olapSvr == null) {
			return false;
		}
		try {
			IEssCube cube = olapSvr.getApplication(appName).getCube(cubeName);
			cube.start();
		} catch (EssException e) {
			LOGGER.error("An error occured while starting cube for " + cubeName, e);
			return false;
		}
		return isSuccess;
	}

	protected static boolean startEssApplication(String appName) {
		boolean isSuccess = true;
		if (olapSvr == null) {
			return false;
		}
		try {
			IEssOlapApplication app = olapSvr.getApplication(appName);
			app.start();
		} catch (EssException e) {
			LOGGER.error("An error occured while starting application for " + appName, e);
			return false;
		}
		return isSuccess;
	}

	protected static boolean stopEssApplication(String appName) {
		boolean isSuccess = true;
		if (olapSvr == null) {
			return false;
		}
		try {
			IEssOlapApplication app = olapSvr.getApplication(appName);
			app.stop();
		} catch (EssException e) {
			LOGGER.error("An error occured while stopping application for " + appName, e);
			return false;
		}
		return isSuccess;
	}

	protected static void addInstanceForParallelDataload(String rulesFile, int dataFileType, String dataFile,
			boolean abortOnError, String dbUser, String dbPassword) throws EssException {
		if (ess != null) {
			ess.addInstanceForParallelDataload(rulesFile, dataFileType, dataFile, abortOnError, 0L, 2L, dbUser,
					dbPassword, 1L, 1L, 0L);
		}
	}

	protected static void setCommonBufferTermOptions() throws EssException {
		if (ess != null) {
			ess.setCommonBufferTermOptions(1L, 1L, 0L);
		}
	}

	protected static String[][] startParallelDataLoad(String appName, String cubeName, int StartBufferId)
			throws EssException {
		if (ess != null) {
			return ess.startParallelDataload(esbserverName, esbUserName, esbpassword, false, esbprovider, appName,
					cubeName, StartBufferId);
		}
		return (String[][]) null;
	}

	protected static String[][] getStatusForDataload() throws EssException {
		return ess.getStatusForDataload();
	}

	protected static void clearInstancesForDataload() throws EssException {
		if (ess != null) {
			ess.clearInstancesForDataload();
		}
	}

	protected String[][] getCubeOnlyVariables(String app, String cube) {
		String[][] subvar = (String[][]) null;
		if ((app != null) && (cube != null)) {
			try {
				if (olapSvr != null) {
					IEssCube olapCube = olapSvr.getApplication(app).getCube(cube);
					subvar = olapCube.getSubstitutionVariables();
				}
			} catch (EssException e) {
				LOGGER.error("An exception occured in getting substitution var", e);
				throw new BaseException(e.getMessage());
			}
		} else {
			return (String[][]) null;
		}
		return subvar;
	}

	protected boolean setSubVariable(String app, String cube, String name, String value) {
		boolean isSuccess = true;
		try {
			if (olapSvr != null) {
				IEssCube essCube = olapSvr.getApplication(app).getCube(cube);
				essCube.createSubstitutionVariable(name, value);
				LOGGER.info("Set substitution variable : Variable=" + name + " Value=" + value);
			}
		} catch (EssException e) {
			isSuccess = false;
			LOGGER.info("An exception occured in setting substitution var");
		}
		return isSuccess;
	}

	protected static boolean executeMaxL(String[] queries) {
		boolean isSuccess = true;
		IEssMaxlSession maxlSess = null;
		try {
			if (olapSvr != null) {
				maxlSess = olapSvr.openMaxlSession("Maxl Test");
				for (int i = 0; i < queries.length; i++) {
					LOGGER.info("Executing Statement: " + queries[i]);
					boolean bMaxlExecuted = maxlSess.execute(queries[i]);
					if (bMaxlExecuted) {
						isSuccess = true;
					}
				}
				maxlSess.close();
			}
		} catch (EssException e) {
			isSuccess = false;
			LOGGER.error("An exception occured in executing MaxL Query", e);
		}
		return isSuccess;
	}

	protected static String[][] startParallelDataLoadBSO(String rulesFile, String dataFile, int dataFileType,
			boolean abortOnError, IEssCube cube) {
		IEssCubeDataloadInstance instance = null;
		String[][] loadErrors = (String[][]) null;
		try {
			if (cube != null) {
				instance = cube.createDataloadInstance();
				instance.setRulesFile(rulesFile);
				instance.setDataFile(dataFile);
				instance.setDataFileType(dataFileType);
				instance.setAbortOnError(abortOnError);

				loadErrors = cube.loadDataParallel(instance);
			}
		} catch (EssException e) {
			LOGGER.info("An exception occured in BSO parallel BSO operation");
		}
		return loadErrors;
	}

	protected boolean executeReplication(IEssCube targetCube, Boolean updatedOnly) {
		boolean isSuccess = false;
		EssPartitionInfo[] parts = null;
		short opType = 1;
		short dirType = 2;
		short metadirType = 3;
		try {
			parts = targetCube.getPartitionList(opType, dirType, metadirType);
			if (parts == null) {
				LOGGER.info("No partition information found for given combination");
				isSuccess = false;
			}
			targetCube.refreshReplicatedPartition(parts[0], updatedOnly.booleanValue());
			isSuccess = true;
		} catch (EssException e) {
			LOGGER.error("Replication execution failed for partition refresh", e);
		}
		return isSuccess;
	}
}

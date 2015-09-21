package de.cimt.talendcomp.tabletransfer;

import java.io.File;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import dbtools.DatabaseSessionPool;

public class Main {

	private static final Logger logger = Logger.getLogger(Main.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Logger.getRootLogger().addAppender(new ConsoleAppender());
		if (args == null || args.length == 0) {
			logger.error("Properties files given as arguments missing!");
			System.exit(1);
		}
		for (int i = 1; i < 2; i++) {
			System.out.println("###################################################################");
			TableTransfer transfer = new TableTransfer();
			if (args == null) {
				logger.error("No properties files given!");
				System.exit(1);
			} else {
				for (String arg : args) {
					File propertyFile = new File(arg);
					if (propertyFile.canRead()) {
						transfer.loadProperties(arg);
					} else {
						logger.error("Unable to read property file: " + arg);
						System.exit(1);
					}
				}
				try {
					transfer.connect();
				} catch (Exception e1) {
					logger.error("connect failed:" + e1.getMessage(), e1);
					System.exit(1);
				}
				try {
					transfer.setupExecute();
				} catch (Exception e1) {
					logger.error("connect failed:" + e1.getMessage(), e1);
					System.exit(1);
				}
				try {
					transfer.execute();
				} catch (Exception e1) {
					logger.error("execute failed:" + e1.getMessage(), e1);
					System.exit(1);
				}
				while (transfer.isRunning()) {
					long duration = System.currentTimeMillis() - transfer.getStartTime();
					double d = duration / 1000l;
					if (d > 0) {
						double insertsPerSecond = transfer.getCurrentCountInserts() / d;
						logger.info("Current read:" + transfer.getCurrentCountReads() + ", inserts: " + transfer.getCurrentCountInserts() + ", rate inserts:" + insertsPerSecond + " rows/s, pool size:" + DatabaseSessionPool.getPoolSize());
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						transfer.stop();
					}
				}
				long duration = System.currentTimeMillis() - transfer.getStartTime();
				long insertsPerSecond = transfer.getCurrentCountInserts() / (duration / 1000l);
				logger.info("Current inserts: " + transfer.getCurrentCountInserts() + " rate inserts:" + insertsPerSecond + " rows/s");
				logger.info("Program ends with return code:" + transfer.getReturnCode());
			}
		}
		System.exit(0);
	}

}

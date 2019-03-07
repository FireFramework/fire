package com.zto.bigdata.spark.common.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ProcessUtil {

	private final static Logger LOGGER = Logger.getLogger(ProcessUtil.class);

	public static int execAndWaitFor(String... command) throws Exception {
		int exitValue = -1;
		Process p = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.redirectErrorStream();
			p = pb.start();
			drainInputStreams(p.getInputStream());
			errorInputStreams(p.getErrorStream());

			p.waitFor();
			exitValue = p.exitValue();
		} finally {
			if (p != null)
				p.destroy();
		}
		return exitValue;
	}

	private static void errorInputStreams(InputStream... inputStreams) {
		for (final InputStream inputStream : inputStreams) {
			new Thread() {
				public void run() {
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
					try {
						StringBuffer buffer = new StringBuffer();
						String line = null;
						while ((line = br.readLine()) != null) {
							if (line != null) {
								buffer.append(line);
							}
						}
						if (buffer.length() > 0) {
							LOGGER.info("ProcessUtil errorInputStreams is " + buffer.toString());
						}
					} catch (IOException e) {
						LOGGER.info("ProcessUtil errorInputStreams is " + e.getMessage());
					} finally {
						try {
							if (inputStream != null)
								inputStream.close();
						} catch (IOException e) {
						}
					}
				}
			}.start();
		}
	}

	private static void drainInputStreams(InputStream... inputStreams) {
		for (final InputStream inputStream : inputStreams) {
			new Thread() {
				public void run() {
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
					try {
						String line = null;
						while ((line = br.readLine()) != null) {
							if (line != null) {
								System.out.println(line);
							}
						}
					} catch (IOException e) {
					} finally {
						try {
							if (inputStream != null)
								inputStream.close();
						} catch (IOException e) {
						}
					}
				}
			}.start();
		}
	}

}

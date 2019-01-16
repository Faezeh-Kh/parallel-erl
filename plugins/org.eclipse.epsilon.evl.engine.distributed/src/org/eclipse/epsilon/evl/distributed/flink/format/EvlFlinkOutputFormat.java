/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.format;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlFlinkRichFunction;

/**
 * Sink used for reporting results.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlFlinkOutputFormat extends RichOutputFormat<SerializableEvlResultAtom> {

	private static final long serialVersionUID = 7282312925441901644L;
	
	transient BufferedWriter writer;
	String outputPath;

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (!outputPath.isEmpty()) {
			writer = new BufferedWriter(new FileWriter(outputPath));
		}
	}
	
	@Override
	public void configure(Configuration additionalParameters) {
		Configuration parameters = EvlFlinkRichFunction.getParameters(getRuntimeContext(), additionalParameters);
		outputPath = parameters.getString("outputFile", "");
	}

	@Override
	public void writeRecord(SerializableEvlResultAtom record) throws IOException {
		String text = record.toString()+"\n";
		
		if (writer != null) {
			writer.write(text);
		}
		else {
			System.out.print(text);
		}
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

}

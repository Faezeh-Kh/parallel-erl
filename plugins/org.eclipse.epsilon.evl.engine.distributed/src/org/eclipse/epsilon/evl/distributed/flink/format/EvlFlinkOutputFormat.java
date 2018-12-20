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

import java.io.IOException;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.eclipse.epsilon.common.util.profiling.BenchmarkUtils;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlResultAtom;

/**
 * A sink used for testing and debugging purposes.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlFlinkOutputFormat extends RichOutputFormat<SerializableEvlResultAtom> {

	private static final long serialVersionUID = -8376881617321903525L;
	
	private int i = 0;
	private long startTime, endTime;

	@Override
	public void configure(Configuration parameters) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		startTime = System.nanoTime();
	}

	@Override
	public void writeRecord(SerializableEvlResultAtom record) throws IOException {
		i++;
	}

	@Override
	public void close() throws IOException {
		endTime = System.nanoTime();
		String duration = BenchmarkUtils.formatExecutionTime(endTime-startTime);
		System.out.println("Processed "+i+" jobs in "+duration);
	}
	
}

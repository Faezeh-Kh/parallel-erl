/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleDistributedMasterJMS.WorkerView;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
abstract class AbstractWorker implements AutoCloseable {

	public static final String
		LAST_MESSAGE_PROPERTY = "lastMsg",
		ID_PROPERTY = "wid";
	
	protected String workerID;
	protected final AtomicBoolean finished = new AtomicBoolean(false);
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof WorkerView)) return false;
		return Objects.equals(this.workerID, ((WorkerView)obj).workerID);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(workerID);
	}
	
	@Override
	public String toString() {
		return getClass().getName()+"-"+workerID;
	}
}

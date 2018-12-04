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

import java.util.List;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import org.eclipse.epsilon.evl.distributed.data.SerializableEvlInputAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlFlinkInputFormat extends ParallelIteratorInputFormat<SerializableEvlInputAtom> {

	private static final long serialVersionUID = 1L;

	public EvlFlinkInputFormat(List<SerializableEvlInputAtom> jobs) {
		super(new ParallelFlinkIterator<>(jobs));
	}

}

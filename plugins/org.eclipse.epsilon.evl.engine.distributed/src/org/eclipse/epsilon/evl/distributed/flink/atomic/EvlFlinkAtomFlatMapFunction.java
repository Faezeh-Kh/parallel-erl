/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.atomic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.eclipse.epsilon.evl.distributed.data.*;
import org.eclipse.epsilon.evl.distributed.flink.EvlFlinkRichFunction;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
class EvlFlinkAtomFlatMapFunction extends EvlFlinkRichFunction implements FlatMapFunction<SerializableEvlInputAtom, SerializableEvlResultAtom> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public void flatMap(SerializableEvlInputAtom value, Collector<SerializableEvlResultAtom> out) throws Exception {
		for (SerializableEvlResultAtom result : localModule.evaluateElement(value)) {
			out.collect(result);
		}
	}
}

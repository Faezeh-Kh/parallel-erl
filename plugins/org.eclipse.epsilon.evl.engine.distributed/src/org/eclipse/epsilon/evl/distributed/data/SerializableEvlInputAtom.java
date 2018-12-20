/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.data;

/**
 * Data unit to be used as inputs in distributed processing. No additional
 * information over the base {@linkplain SerializableEvlAtom} is required.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlInputAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = -8506775241534384089L;

	@Override
	protected SerializableEvlInputAtom clone() {
		SerializableEvlInputAtom clone = new SerializableEvlInputAtom();
		clone.modelElementID = ""+this.modelElementID;
		clone.modelName = ""+this.modelName;
		clone.contextName = ""+this.contextName;
		return clone;
	}
}

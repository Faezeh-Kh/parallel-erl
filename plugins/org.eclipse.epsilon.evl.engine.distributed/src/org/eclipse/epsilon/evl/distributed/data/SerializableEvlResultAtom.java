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

import java.util.Objects;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlResultAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = -1980020699534098214L;
	
	public String constraintName, message;
	
	@Override
	protected SerializableEvlResultAtom clone() {
		SerializableEvlResultAtom clone = new SerializableEvlResultAtom();
		clone.constraintName = ""+this.constraintName;
		clone.message = ""+this.message;
		clone.modelElementID = ""+this.modelElementID;
		clone.modelName = ""+this.modelName;
		clone.contextName = ""+this.contextName;
		return clone;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), constraintName, message);
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		
		SerializableEvlResultAtom other = (SerializableEvlResultAtom) obj;
		return
			Objects.equals(this.constraintName, other.constraintName) &&
			Objects.equals(this.message, other.message);
	}

	@Override
	public String toString() {
		return super.toString()+", constraintName=" + constraintName + ", message=" + message;
	}
}

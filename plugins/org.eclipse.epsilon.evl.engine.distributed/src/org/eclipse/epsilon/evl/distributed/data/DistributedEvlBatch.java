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
public class DistributedEvlBatch implements java.io.Serializable, Cloneable {

	private static final long serialVersionUID = 139906878129493833L;
	
	public int from, to;
	
	@Override
	protected DistributedEvlBatch clone() {
		DistributedEvlBatch clone = new DistributedEvlBatch();
		clone.from = this.from;
		clone.to = this.to;
		return clone;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(from, to);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof DistributedEvlBatch)) return false;
		DistributedEvlBatch other = (DistributedEvlBatch) obj;
		return this.from == other.from && this.to == other.to;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+": from="+from+", to="+to;
	}
}

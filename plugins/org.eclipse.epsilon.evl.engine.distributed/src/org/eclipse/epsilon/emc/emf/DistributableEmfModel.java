/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.emc.emf;

/**
 * EmfModel which doesn't cache ResourceSet.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributableEmfModel extends org.eclipse.epsilon.emc.emf.EmfModel {
	
	@Override
	protected org.eclipse.emf.ecore.resource.ResourceSet createResourceSet() {
		return new org.eclipse.emf.ecore.resource.impl.ResourceSetImpl();
	}
}

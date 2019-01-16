package org.eclipse.epsilon.emc.emf;

public class DistributableEmfModel extends org.eclipse.epsilon.emc.emf.EmfModel {
	
	@Override
	protected org.eclipse.emf.ecore.resource.ResourceSet createResourceSet() {
		return new org.eclipse.emf.ecore.resource.impl.ResourceSetImpl();
	}
}

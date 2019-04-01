package org.eclipse.epsilon.evl.distributed.crossflow;


public class ResultSink extends ResultSinkBase {
	
	@Override
	public void consumeValidationOutput(ValidationResult validationResult) throws Exception {
		
		// TODO: Add implementation that instantiates, sets, and submits result objects (example below)
		System.out.println("[" + workflow.getName() + "] Result is " + validationResult.getAtom().toString() + " (cached=" + validationResult.isCached() + ")");
	
	}


}
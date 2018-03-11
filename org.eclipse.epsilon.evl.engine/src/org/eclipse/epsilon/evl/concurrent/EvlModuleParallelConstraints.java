package org.eclipse.epsilon.evl.concurrent;

import java.util.Collection;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.concurrent.ErlExecutorService;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.context.concurrent.IEvlContextParallel;

public class EvlModuleParallelConstraints extends EvlModuleParallel {

	public EvlModuleParallelConstraints() {
		super();
	}
	
	public EvlModuleParallelConstraints(IEvlContextParallel parallelEvlContext) {
		super(parallelEvlContext);
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {
		IEvlContextParallel context = getContext();
		ErlExecutorService executor = context.getExecutor();
		
		for (ConstraintContext constraintContext : getConstraintContexts()) {
			Collection<Constraint> constraintsToCheck = preProcessConstraintContext(constraintContext);
			
			for (Object object : constraintContext.getAllOfSourceKind(context)) {
				if (constraintContext.shouldBeChecked(object, context)) {
					for (Constraint constraint : constraintsToCheck) {
						executor.execute(() -> {
							try {
								constraint.execute(object, context);
							}
							catch (EolRuntimeException ex) {
								context.handleException(ex, executor);
							}
						});
					}
				}
			}
		}
		
		executor.awaitCompletion();
	}

}

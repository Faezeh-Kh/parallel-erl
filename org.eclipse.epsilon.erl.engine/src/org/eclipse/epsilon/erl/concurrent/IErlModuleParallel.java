package org.eclipse.epsilon.erl.concurrent;

import org.eclipse.epsilon.erl.IErlModule;
import org.eclipse.epsilon.erl.execute.context.concurrent.IErlContextParallel;

public interface IErlModuleParallel extends IErlModule {
	
	@Override
	public IErlContextParallel getContext();
}

/*******************************************************************************
 * Copyright (c) 2008 The University of York.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Dimitrios Kolovos - initial API and implementation
 ******************************************************************************/
package org.eclipse.epsilon.evl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.TokenStream;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.common.parse.AST;
import org.eclipse.epsilon.common.parse.EpsilonParser;
import org.eclipse.epsilon.common.util.AstUtil;
import org.eclipse.epsilon.eol.dom.ExecutableBlock;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.context.FrameStack;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.execute.context.Variable;
import org.eclipse.epsilon.erl.ErlModule;
import org.eclipse.epsilon.evl.dom.*;
import org.eclipse.epsilon.evl.execute.EvlOperationFactory;
import org.eclipse.epsilon.evl.execute.context.EvlContext;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;
import org.eclipse.epsilon.evl.execute.exceptions.EvlConstraintNotFoundException;
import org.eclipse.epsilon.evl.graph.EvlGraph;
import org.eclipse.epsilon.evl.parse.EvlLexer;
import org.eclipse.epsilon.evl.parse.EvlParser;

public class EvlModule extends ErlModule implements IEvlModule {
	
	protected IEvlFixer fixer;
	protected List<ConstraintContext> constraintContexts, declaredConstraintContexts;
	protected final Constraints constraints = new Constraints();
	private boolean optimizeConstraints = false;
	
	public EvlModule() {
		this(new EvlContext());
	}
	
	public EvlModule(IEvlContext evlContext) {
		this.context = evlContext;
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.EolLibraryModule#createLexer(org.antlr.runtime.ANTLRInputStream)
	 */
	@Override
	protected Lexer createLexer(ANTLRInputStream inputStream) {
		return new EvlLexer(inputStream);
	}
 
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.EolLibraryModule#createParser(org.antlr.runtime.TokenStream)
	 */
	@Override
	public EpsilonParser createParser(TokenStream tokenStream) {
		return new EvlParser(tokenStream);
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.EolLibraryModule#getMainRule()
	 */
	@Override
	public String getMainRule() {
		return "evlModule";
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.erl.ErlModule#adapt(org.eclipse.epsilon.common.parse.AST, org.eclipse.epsilon.common.module.ModuleElement)
	 */
	@Override
	public ModuleElement adapt(AST cst, ModuleElement parentAst) {
		switch (cst.getType()) {
			case EvlParser.FIX: return new Fix();
			case EvlParser.DO: return new ExecutableBlock<Void>(Void.class);
			case EvlParser.TITLE:
			case EvlParser.MESSAGE:
				return new ExecutableBlock<String>(String.class);
			case EvlParser.CONSTRAINT:
			case EvlParser.CRITIQUE:
				return new Constraint();
			case EvlParser.CONTEXT:
				return new ConstraintContext();
			case EvlParser.CHECK:
			case EvlParser.GUARD:
				return new ExecutableBlock<Boolean>(Boolean.class);
		}
		return super.adapt(cst, parentAst);
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.EolLibraryModule#getImportConfiguration()
	 */
	@Override
	public HashMap<String, Class<?>> getImportConfiguration() {
		HashMap<String, Class<?>> importConfiguration = super.getImportConfiguration();
		importConfiguration.put("evl", EvlModule.class);
		return importConfiguration;
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.erl.ErlModule#build(org.eclipse.epsilon.common.parse.AST, org.eclipse.epsilon.common.module.IModule)
	 */
	@Override
	public void build(AST cst, IModule module) {
		super.build(cst, module);
		
		ConstraintContext globalConstraintContext = new GlobalConstraintContext();
		globalConstraintContext.setModule(this);
		globalConstraintContext.setParent(this);
		this.getChildren().add(globalConstraintContext);
		
		Constraints globalConstraints = globalConstraintContext.getConstraints();
		
		List<AST>
			constraintASTs = AstUtil.getChildren(cst, EvlParser.CONSTRAINT),
			critiqueASTs = AstUtil.getChildren(cst, EvlParser.CRITIQUE);
		
		globalConstraints.ensureCapacity(constraintASTs.size()+critiqueASTs.size()+globalConstraints.size());
		
		for (AST constraintAst : constraintASTs) {
			Constraint constraint = (Constraint) module.createAst(constraintAst, globalConstraintContext);
			globalConstraints.add(constraint); 
			constraint.setConstraintContext(globalConstraintContext);
		}
		
		for (AST critiqueAst : critiqueASTs) {
			Constraint critique = (Constraint) module.createAst(critiqueAst, globalConstraintContext);
			globalConstraints.add(critique); 
			critique.setConstraintContext(globalConstraintContext);
		}
		
		Collection<AST> constraintContextAsts = AstUtil.getChildren(cst, EvlParser.CONTEXT);
		declaredConstraintContexts = new ArrayList<>(constraintContextAsts.size());
		
		for (AST constraintContextAst : constraintContextAsts) {
			declaredConstraintContexts.add((ConstraintContext) module.createAst(constraintContextAst, this));
		}

		if (!globalConstraints.isEmpty()) {
			declaredConstraintContexts.add(globalConstraintContext);
		}
		
		// Cache all the constraints
		for (ConstraintContext constraintContext : getConstraintContexts()) {
			constraints.addAll(constraintContext.getConstraints());
		}
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.evl.IEvlModule#getDeclaredConstraintContexts()
	 */
	@Override
	public List<ConstraintContext> getDeclaredConstraintContexts() {
		return declaredConstraintContexts;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.evl.IEvlModule#getConstraintContexts()
	 */
	@Override
	public List<ConstraintContext> getConstraintContexts() {
		if (constraintContexts == null) {
			constraintContexts = imports.stream()
				.filter(imp -> imp.isLoaded() && (imp.getModule() instanceof IEvlModule))
				.map(imp -> ((IEvlModule)imp.getModule()).getConstraintContexts())
				.flatMap(List::stream)
				.collect(Collectors.toList()
			);
			constraintContexts.addAll(declaredConstraintContexts);
		}
		return constraintContexts;
	}
	
	protected List<Constraint> computeConstraintSequence() throws EvlConstraintNotFoundException {
		IEvlContext context = getContext();
		EvlGraph graph = new EvlGraph(context);
		graph.addConstraintContexts(getConstraintContexts());
		context.setConstraintsDependedOn(graph.getAllConstraintsDependedOn());
		return graph.getConstraintSequence();
	}
	
	protected Collection<Constraint> getOptimisedConstraintsFor(ConstraintContext constraintContext) throws EolRuntimeException {
		IEvlContext context = getContext();
		Collection<Constraint>
			dependedOn = context.getConstraintsDependedOn(),
			remainingConstraints = new ArrayList<>(constraintContext.getConstraints());
		ConstraintSelectTransfomer transformer = new ConstraintSelectTransfomer();
		
		for (Iterator<Constraint> itConstraint = remainingConstraints.iterator(); itConstraint.hasNext();) {
			Constraint constraint = itConstraint.next();
			if (transformer.canBeTransformed(constraint) && !constraint.isLazy(context)) {
				ExecutableBlock<?> transformedConstraint = transformer.transformIntoSelect(constraint);
				Iterable<?> results = (Iterable<?>) transformedConstraint.execute(context);

				// Postprocess the invalid objects to support custom messages and fix blocks
				for (Object self : results) {
					// We know result = false because we found it with the negated condition
					constraint.optimisedCheck(self, context, false);
				}

				//If we already know the result won't be used, don't bother adding it to the trace!
				if (dependedOn == null || dependedOn.contains(constraint)) {
					// Mark this constraint as executed in an optimised way: we will only have
					// explicit trace items for invalid objects, so we'll have to tweak isChecked
					// and isSatisfied accordingly.
					context.getConstraintTrace().addCheckedOptimised(constraint);
				}

				// Don't try to reexecute this rule later on
				itConstraint.remove();
			}
		}
		
		return remainingConstraints;
	}
	
	protected Collection<Constraint> preProcessConstraintContext(ConstraintContext constraintContext) throws EolRuntimeException {
		return optimizeConstraints ? getOptimisedConstraintsFor(constraintContext) : constraintContext.getConstraints();
	}
	
	@Override
	protected void prepareContext() {
		super.prepareContext();
		IEvlContext context = getContext();
		context.setOperationFactory(new EvlOperationFactory());
		FrameStack fs = context.getFrameStack();
		fs.put(Variable.createReadOnlyVariable("constraintTrace", context.getConstraintTrace()));
		fs.put(Variable.createReadOnlyVariable("thisModule", this));
	}

	/*
	 * Invokes the execute() method on all Constraints in all ConstraintContexts.
	 * If optimizeConstraints, the constraints to be checked are filtered.
	 */
	protected void checkConstraints() throws EolRuntimeException {
		IEvlContext context = getContext();
		
		for (ConstraintContext constraintContext : getConstraintContexts()) {
			constraintContext.checkAll(context, preProcessConstraintContext(constraintContext));
		}
	}
	
	/*
	 * Clean up, execute fixes and post block.
	 */
	@Override
	protected void postExecution() throws EolRuntimeException {
		if (fixer != null) {
			fixer.fix(this);
		}
		super.postExecution();
	}
	
	@Override
	public Object executeImpl() throws EolRuntimeException {
		prepareExecution();
		checkConstraints();
		postExecution();
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.IEolLibraryModule#getContext()
	 */
	@Override
	public IEvlContext getContext() {
		return (IEvlContext) context;
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.evl.IEvlModule#getConstraints()
	 */
	public Constraints getConstraints() { 
		return constraints;
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.evl.IEvlModule#setUnsatisfiedConstraintFixer(org.eclipse.epsilon.evl.IEvlFixer)
	 */
	public void setUnsatisfiedConstraintFixer(IEvlFixer fixer) {
		this.fixer = fixer;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.evl.IEvlModule#getUnsatisfiedConstraintFixer()
	 */
	public IEvlFixer getUnsatisfiedConstraintFixer() {
		return fixer;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.erl.ErlModule#getPostBlockTokenType()
	 */
	@Override
	protected int getPostBlockTokenType() {
		return EvlParser.POST;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.erl.ErlModule#getPreBlockTokenType()
	 */
	@Override
	protected int getPreBlockTokenType() {
		return EvlParser.PRE;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.epsilon.eol.IEolLibraryModule#setContext(org.eclipse.epsilon.eol.execute.context.IEolContext)
	 */
	@Override
	public void setContext(IEolContext context) {
		if (context instanceof IEvlContext) {
			super.setContext(context);
		}
	}
	
	/**
	 * Checks if is optimize constraints.
	 *
	 * @return true, if is optimize constraints
	 */
	public boolean isOptimizeConstraints() {
		return optimizeConstraints;
	}

	/**
	 * Sets the optimize constraints.
	 *
	 * @param optimizeConstraints the new optimize constraints
	 */
	public void setOptimizeConstraints(boolean optimizeConstraints) {
		this.optimizeConstraints = optimizeConstraints;
	}
}

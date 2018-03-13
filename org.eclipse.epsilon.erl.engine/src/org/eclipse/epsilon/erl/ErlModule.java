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
package org.eclipse.epsilon.erl;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.epsilon.common.module.IModule;
import org.eclipse.epsilon.common.module.ModuleElement;
import org.eclipse.epsilon.common.parse.AST;
import org.eclipse.epsilon.common.parse.problem.ParseProblem;
import org.eclipse.epsilon.common.util.AstUtil;
import org.eclipse.epsilon.eol.EolModule;
import org.eclipse.epsilon.eol.dom.Import;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.execute.ExecutorFactory;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.erl.dom.*;
import org.eclipse.epsilon.erl.exceptions.ErlCircularRuleInheritanceException;
import org.eclipse.epsilon.erl.exceptions.ErlRuleNotFoundException;

public abstract class ErlModule extends EolModule implements IErlModule {
	
	protected NamedRuleList<Pre> pre, declaredPre = new NamedRuleList<>();
	protected NamedRuleList<Post> post, declaredPost = new NamedRuleList<>();
	
	@Override
	public void build(AST cst, IModule module) {
		super.build(cst, module);
		
		List<AST>
			preBlockASTs = AstUtil.getChildren(cst, getPreBlockTokenType()),
			postBlockASTs = AstUtil.getChildren(cst, getPostBlockTokenType());
		
		declaredPre.ensureCapacity(preBlockASTs.size());
		declaredPost.ensureCapacity(preBlockASTs.size());
		
		for (AST preBlockAst : preBlockASTs){
			declaredPre.add((Pre) module.createAst(preBlockAst, this));
		}
		
		for (AST postBlockAst : postBlockASTs) {
			declaredPost.add((Post) module.createAst(postBlockAst, this));
		}
	}
	
	@Override
	public List<Post> getPost() {
		if (post == null) {
			post = new NamedRuleList<>();
			for (Import import_ : imports) {
				if (import_.isLoaded() && (import_.getModule() instanceof IErlModule)) {
					IErlModule module = (IErlModule) import_.getModule();
					post.addAll(module.getPost());
				}
			}
			post.addAll(declaredPost);
			
		}
		return post;
	}

	@Override
	public List<Pre> getPre() {
		if (pre == null) {
			pre = new NamedRuleList<>();
			for (Import import_ : imports) {
				if (import_.isLoaded() && (import_.getModule() instanceof IErlModule)) {
					IErlModule module = (IErlModule) import_.getModule();
					pre.addAll(module.getPre());
				}
			}
			pre.addAll(declaredPre);
		}
		return pre;
	}

	@Override
	public ModuleElement adapt(AST cst, ModuleElement parentAst) {
		if (cst.getType() == getPreBlockTokenType()) {
			return new Pre();
		}
		else if (cst.getType() == getPostBlockTokenType()) {
			return new Post();
		}
		return super.adapt(cst, parentAst);
	}
	
	@Override
	public List<Post> getDeclaredPost() {
		return declaredPost;
	}

	@Override
	public List<Pre> getDeclaredPre() {
		return declaredPre;
	}
	
	protected void prepareExecution() throws EolRuntimeException {
		prepareContext();
		execute(getPre(), context);
	}
	
	protected void postExecution() throws EolRuntimeException {
		execute(getPost(), context);
	}
	
	protected void execute(List<? extends NamedStatementBlockRule> namedRules) throws EolRuntimeException {
		execute(namedRules, getContext());
	}
	
	protected void execute(List<? extends NamedStatementBlockRule> namedRules, IEolContext context) throws EolRuntimeException {
		ExecutorFactory executorFactory = context.getExecutorFactory();
		for (NamedStatementBlockRule namedRule : namedRules) {
			executorFactory.execute(namedRule.getBody(), context);
		}
	}
	
	protected abstract int getPreBlockTokenType();
	protected abstract int getPostBlockTokenType();

	public List<ParseProblem> calculateSuperRules(List<? extends ExtensibleNamedRule> allRules) {
		List<ParseProblem> parseProblems = new ArrayList<>();
		for (ExtensibleNamedRule rule : allRules) {
			try {
				rule.calculateSuperRules(allRules);
			}
			catch (ErlRuleNotFoundException | ErlCircularRuleInheritanceException e) {
				ParseProblem problem = new ParseProblem();
				problem.setLine(rule.getRegion().getStart().getLine());
				problem.setReason(e.getReason());
				parseProblems.add(problem);
			}
		}
		return parseProblems;
	}
}

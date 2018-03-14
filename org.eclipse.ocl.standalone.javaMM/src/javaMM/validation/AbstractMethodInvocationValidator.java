/**
 *
 * $Id$
 */
package javaMM.validation;

import javaMM.AbstractMethodDeclaration;
import javaMM.Expression;
import javaMM.TypeAccess;

import org.eclipse.emf.common.util.EList;

/**
 * A sample validator interface for {@link javaMM.AbstractMethodInvocation}.
 * This doesn't really do anything, and it's not a real EMF artifact.
 * It was generated by the org.eclipse.emf.examples.generator.validator plug-in to illustrate how EMF's code generator can be extended.
 * This can be disabled with -vmargs -Dorg.eclipse.emf.examples.generator.validator=false.
 */
public interface AbstractMethodInvocationValidator {
	boolean validate();

	boolean validateMethod(AbstractMethodDeclaration value);
	boolean validateArguments(EList<Expression> value);
	boolean validateTypeArguments(EList<TypeAccess> value);
}
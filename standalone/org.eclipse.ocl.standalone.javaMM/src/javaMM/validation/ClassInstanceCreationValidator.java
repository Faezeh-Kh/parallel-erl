/**
 *
 * $Id$
 */
package javaMM.validation;

import javaMM.AnonymousClassDeclaration;
import javaMM.Expression;
import javaMM.TypeAccess;

/**
 * A sample validator interface for {@link javaMM.ClassInstanceCreation}.
 * This doesn't really do anything, and it's not a real EMF artifact.
 * It was generated by the org.eclipse.emf.examples.generator.validator plug-in to illustrate how EMF's code generator can be extended.
 * This can be disabled with -vmargs -Dorg.eclipse.emf.examples.generator.validator=false.
 */
public interface ClassInstanceCreationValidator {
	boolean validate();

	boolean validateAnonymousClassDeclaration(AnonymousClassDeclaration value);
	boolean validateExpression(Expression value);
	boolean validateType(TypeAccess value);
}
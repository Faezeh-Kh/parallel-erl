/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Labeled Statement</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.LabeledStatement#getBody <em>Body</em>}</li>
 *   <li>{@link javaMM.LabeledStatement#getUsagesInBreakStatements <em>Usages In Break Statements</em>}</li>
 *   <li>{@link javaMM.LabeledStatement#getUsagesInContinueStatements <em>Usages In Continue Statements</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getLabeledStatement()
 * @model
 * @generated
 */
public interface LabeledStatement extends NamedElement, Statement {
	/**
	 * Returns the value of the '<em><b>Body</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Body</em>' containment reference.
	 * @see #setBody(Statement)
	 * @see javaMM.JavaMMPackage#getLabeledStatement_Body()
	 * @model containment="true" required="true"
	 * @generated
	 */
	Statement getBody();

	/**
	 * Sets the value of the '{@link javaMM.LabeledStatement#getBody <em>Body</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Body</em>' containment reference.
	 * @see #getBody()
	 * @generated
	 */
	void setBody(Statement value);

	/**
	 * Returns the value of the '<em><b>Usages In Break Statements</b></em>' reference list.
	 * The list contents are of type {@link javaMM.BreakStatement}.
	 * It is bidirectional and its opposite is '{@link javaMM.BreakStatement#getLabel <em>Label</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages In Break Statements</em>' reference list.
	 * @see javaMM.JavaMMPackage#getLabeledStatement_UsagesInBreakStatements()
	 * @see javaMM.BreakStatement#getLabel
	 * @model opposite="label" ordered="false"
	 * @generated
	 */
	EList<BreakStatement> getUsagesInBreakStatements();

	/**
	 * Returns the value of the '<em><b>Usages In Continue Statements</b></em>' reference list.
	 * The list contents are of type {@link javaMM.ContinueStatement}.
	 * It is bidirectional and its opposite is '{@link javaMM.ContinueStatement#getLabel <em>Label</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages In Continue Statements</em>' reference list.
	 * @see javaMM.JavaMMPackage#getLabeledStatement_UsagesInContinueStatements()
	 * @see javaMM.ContinueStatement#getLabel
	 * @model opposite="label" ordered="false"
	 * @generated
	 */
	EList<ContinueStatement> getUsagesInContinueStatements();

} // LabeledStatement

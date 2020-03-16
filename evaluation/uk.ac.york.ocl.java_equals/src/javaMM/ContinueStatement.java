/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Continue Statement</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.ContinueStatement#getLabel <em>Label</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getContinueStatement()
 * @model
 * @generated
 */
public interface ContinueStatement extends Statement {
	/**
	 * Returns the value of the '<em><b>Label</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link javaMM.LabeledStatement#getUsagesInContinueStatements <em>Usages In Continue Statements</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Label</em>' reference.
	 * @see #setLabel(LabeledStatement)
	 * @see javaMM.JavaMMPackage#getContinueStatement_Label()
	 * @see javaMM.LabeledStatement#getUsagesInContinueStatements
	 * @model opposite="usagesInContinueStatements"
	 * @generated
	 */
	LabeledStatement getLabel();

	/**
	 * Sets the value of the '{@link javaMM.ContinueStatement#getLabel <em>Label</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Label</em>' reference.
	 * @see #getLabel()
	 * @generated
	 */
	void setLabel(LabeledStatement value);

} // ContinueStatement

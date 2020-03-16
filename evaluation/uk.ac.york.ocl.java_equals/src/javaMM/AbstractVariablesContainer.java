/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Abstract Variables Container</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AbstractVariablesContainer#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.AbstractVariablesContainer#getFragments <em>Fragments</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAbstractVariablesContainer()
 * @model abstract="true"
 * @generated
 */
public interface AbstractVariablesContainer extends ASTNode {
	/**
	 * Returns the value of the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' containment reference.
	 * @see #setType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getAbstractVariablesContainer_Type()
	 * @model containment="true"
	 * @generated
	 */
	TypeAccess getType();

	/**
	 * Sets the value of the '{@link javaMM.AbstractVariablesContainer#getType <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' containment reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(TypeAccess value);

	/**
	 * Returns the value of the '<em><b>Fragments</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.VariableDeclarationFragment}.
	 * It is bidirectional and its opposite is '{@link javaMM.VariableDeclarationFragment#getVariablesContainer <em>Variables Container</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Fragments</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAbstractVariablesContainer_Fragments()
	 * @see javaMM.VariableDeclarationFragment#getVariablesContainer
	 * @model opposite="variablesContainer" containment="true"
	 * @generated
	 */
	EList<VariableDeclarationFragment> getFragments();

} // AbstractVariablesContainer

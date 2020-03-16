/**
 */
package javaMM;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Declaration Fragment</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.VariableDeclarationFragment#getVariablesContainer <em>Variables Container</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getVariableDeclarationFragment()
 * @model
 * @generated
 */
public interface VariableDeclarationFragment extends VariableDeclaration {
	/**
	 * Returns the value of the '<em><b>Variables Container</b></em>' container reference.
	 * It is bidirectional and its opposite is '{@link javaMM.AbstractVariablesContainer#getFragments <em>Fragments</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Variables Container</em>' container reference.
	 * @see #setVariablesContainer(AbstractVariablesContainer)
	 * @see javaMM.JavaMMPackage#getVariableDeclarationFragment_VariablesContainer()
	 * @see javaMM.AbstractVariablesContainer#getFragments
	 * @model opposite="fragments" transient="false"
	 * @generated
	 */
	AbstractVariablesContainer getVariablesContainer();

	/**
	 * Sets the value of the '{@link javaMM.VariableDeclarationFragment#getVariablesContainer <em>Variables Container</em>}' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variables Container</em>' container reference.
	 * @see #getVariablesContainer()
	 * @generated
	 */
	void setVariablesContainer(AbstractVariablesContainer value);

} // VariableDeclarationFragment

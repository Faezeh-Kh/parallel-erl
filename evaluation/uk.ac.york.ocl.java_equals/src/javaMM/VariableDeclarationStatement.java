/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Declaration Statement</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.VariableDeclarationStatement#getExtraArrayDimensions <em>Extra Array Dimensions</em>}</li>
 *   <li>{@link javaMM.VariableDeclarationStatement#getModifier <em>Modifier</em>}</li>
 *   <li>{@link javaMM.VariableDeclarationStatement#getAnnotations <em>Annotations</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getVariableDeclarationStatement()
 * @model
 * @generated
 */
public interface VariableDeclarationStatement extends Statement, AbstractVariablesContainer {
	/**
	 * Returns the value of the '<em><b>Extra Array Dimensions</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #setExtraArrayDimensions(int)
	 * @see javaMM.JavaMMPackage#getVariableDeclarationStatement_ExtraArrayDimensions()
	 * @model required="true"
	 * @generated
	 */
	int getExtraArrayDimensions();

	/**
	 * Sets the value of the '{@link javaMM.VariableDeclarationStatement#getExtraArrayDimensions <em>Extra Array Dimensions</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Extra Array Dimensions</em>' attribute.
	 * @see #getExtraArrayDimensions()
	 * @generated
	 */
	void setExtraArrayDimensions(int value);

	/**
	 * Returns the value of the '<em><b>Modifier</b></em>' containment reference.
	 * It is bidirectional and its opposite is '{@link javaMM.Modifier#getVariableDeclarationStatement <em>Variable Declaration Statement</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Modifier</em>' containment reference.
	 * @see #setModifier(Modifier)
	 * @see javaMM.JavaMMPackage#getVariableDeclarationStatement_Modifier()
	 * @see javaMM.Modifier#getVariableDeclarationStatement
	 * @model opposite="variableDeclarationStatement" containment="true"
	 * @generated
	 */
	Modifier getModifier();

	/**
	 * Sets the value of the '{@link javaMM.VariableDeclarationStatement#getModifier <em>Modifier</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Modifier</em>' containment reference.
	 * @see #getModifier()
	 * @generated
	 */
	void setModifier(Modifier value);

	/**
	 * Returns the value of the '<em><b>Annotations</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.Annotation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Annotations</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getVariableDeclarationStatement_Annotations()
	 * @model containment="true"
	 * @generated
	 */
	EList<Annotation> getAnnotations();

} // VariableDeclarationStatement

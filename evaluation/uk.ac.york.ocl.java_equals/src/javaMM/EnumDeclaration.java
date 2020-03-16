/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Enum Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.EnumDeclaration#getEnumConstants <em>Enum Constants</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getEnumDeclaration()
 * @model
 * @generated
 */
public interface EnumDeclaration extends AbstractTypeDeclaration {
	/**
	 * Returns the value of the '<em><b>Enum Constants</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.EnumConstantDeclaration}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enum Constants</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getEnumDeclaration_EnumConstants()
	 * @model containment="true"
	 * @generated
	 */
	EList<EnumConstantDeclaration> getEnumConstants();

} // EnumDeclaration

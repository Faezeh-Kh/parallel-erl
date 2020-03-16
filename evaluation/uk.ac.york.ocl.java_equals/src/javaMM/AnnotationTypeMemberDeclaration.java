/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Annotation Type Member Declaration</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.AnnotationTypeMemberDeclaration#getDefault <em>Default</em>}</li>
 *   <li>{@link javaMM.AnnotationTypeMemberDeclaration#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.AnnotationTypeMemberDeclaration#getUsages <em>Usages</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAnnotationTypeMemberDeclaration()
 * @model
 * @generated
 */
public interface AnnotationTypeMemberDeclaration extends BodyDeclaration {
	/**
	 * Returns the value of the '<em><b>Default</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Default</em>' containment reference.
	 * @see #setDefault(Expression)
	 * @see javaMM.JavaMMPackage#getAnnotationTypeMemberDeclaration_Default()
	 * @model containment="true"
	 * @generated
	 */
	Expression getDefault();

	/**
	 * Sets the value of the '{@link javaMM.AnnotationTypeMemberDeclaration#getDefault <em>Default</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Default</em>' containment reference.
	 * @see #getDefault()
	 * @generated
	 */
	void setDefault(Expression value);

	/**
	 * Returns the value of the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' containment reference.
	 * @see #setType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getAnnotationTypeMemberDeclaration_Type()
	 * @model containment="true" required="true"
	 * @generated
	 */
	TypeAccess getType();

	/**
	 * Sets the value of the '{@link javaMM.AnnotationTypeMemberDeclaration#getType <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' containment reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(TypeAccess value);

	/**
	 * Returns the value of the '<em><b>Usages</b></em>' reference list.
	 * The list contents are of type {@link javaMM.AnnotationMemberValuePair}.
	 * It is bidirectional and its opposite is '{@link javaMM.AnnotationMemberValuePair#getMember <em>Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Usages</em>' reference list.
	 * @see javaMM.JavaMMPackage#getAnnotationTypeMemberDeclaration_Usages()
	 * @see javaMM.AnnotationMemberValuePair#getMember
	 * @model opposite="member" ordered="false"
	 * @generated
	 */
	EList<AnnotationMemberValuePair> getUsages();

} // AnnotationTypeMemberDeclaration

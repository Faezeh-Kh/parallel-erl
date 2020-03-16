/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Annotation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Annotation#getType <em>Type</em>}</li>
 *   <li>{@link javaMM.Annotation#getValues <em>Values</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getAnnotation()
 * @model
 * @generated
 */
public interface Annotation extends Expression {
	/**
	 * Returns the value of the '<em><b>Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Type</em>' containment reference.
	 * @see #setType(TypeAccess)
	 * @see javaMM.JavaMMPackage#getAnnotation_Type()
	 * @model containment="true" required="true"
	 * @generated
	 */
	TypeAccess getType();

	/**
	 * Sets the value of the '{@link javaMM.Annotation#getType <em>Type</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Type</em>' containment reference.
	 * @see #getType()
	 * @generated
	 */
	void setType(TypeAccess value);

	/**
	 * Returns the value of the '<em><b>Values</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.AnnotationMemberValuePair}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Values</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getAnnotation_Values()
	 * @model containment="true"
	 * @generated
	 */
	EList<AnnotationMemberValuePair> getValues();

} // Annotation

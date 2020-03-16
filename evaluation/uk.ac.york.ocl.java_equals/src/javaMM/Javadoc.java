/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Javadoc</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Javadoc#getTags <em>Tags</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getJavadoc()
 * @model
 * @generated
 */
public interface Javadoc extends Comment {
	/**
	 * Returns the value of the '<em><b>Tags</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.TagElement}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tags</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getJavadoc_Tags()
	 * @model containment="true"
	 * @generated
	 */
	EList<TagElement> getTags();

} // Javadoc

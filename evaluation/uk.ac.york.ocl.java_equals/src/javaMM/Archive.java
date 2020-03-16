/**
 */
package javaMM;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Archive</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link javaMM.Archive#getOriginalFilePath <em>Original File Path</em>}</li>
 *   <li>{@link javaMM.Archive#getClassFiles <em>Class Files</em>}</li>
 *   <li>{@link javaMM.Archive#getManifest <em>Manifest</em>}</li>
 * </ul>
 *
 * @see javaMM.JavaMMPackage#getArchive()
 * @model
 * @generated
 */
public interface Archive extends NamedElement {
	/**
	 * Returns the value of the '<em><b>Original File Path</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Original File Path</em>' attribute.
	 * @see #setOriginalFilePath(String)
	 * @see javaMM.JavaMMPackage#getArchive_OriginalFilePath()
	 * @model required="true"
	 * @generated
	 */
	String getOriginalFilePath();

	/**
	 * Sets the value of the '{@link javaMM.Archive#getOriginalFilePath <em>Original File Path</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Original File Path</em>' attribute.
	 * @see #getOriginalFilePath()
	 * @generated
	 */
	void setOriginalFilePath(String value);

	/**
	 * Returns the value of the '<em><b>Class Files</b></em>' containment reference list.
	 * The list contents are of type {@link javaMM.ClassFile}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Class Files</em>' containment reference list.
	 * @see javaMM.JavaMMPackage#getArchive_ClassFiles()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<ClassFile> getClassFiles();

	/**
	 * Returns the value of the '<em><b>Manifest</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Manifest</em>' containment reference.
	 * @see #setManifest(Manifest)
	 * @see javaMM.JavaMMPackage#getArchive_Manifest()
	 * @model containment="true"
	 * @generated
	 */
	Manifest getManifest();

	/**
	 * Sets the value of the '{@link javaMM.Archive#getManifest <em>Manifest</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Manifest</em>' containment reference.
	 * @see #getManifest()
	 * @generated
	 */
	void setManifest(Manifest value);

} // Archive

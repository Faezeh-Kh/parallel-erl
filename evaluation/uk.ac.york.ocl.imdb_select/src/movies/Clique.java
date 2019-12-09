/**
 */
package movies;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Clique</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link movies.Clique#getPersons <em>Persons</em>}</li>
 * </ul>
 *
 * @see movies.MoviesPackage#getClique()
 * @model
 * @generated
 */
public interface Clique extends Group {
	/**
	 * Returns the value of the '<em><b>Persons</b></em>' reference list.
	 * The list contents are of type {@link movies.Person}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Persons</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Persons</em>' reference list.
	 * @see movies.MoviesPackage#getClique_Persons()
	 * @model
	 * @generated
	 */
	EList<Person> getPersons();

} // Clique

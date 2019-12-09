/**
 */
package movies;

import java.math.BigInteger;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Person</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link movies.Person#getMovies <em>Movies</em>}</li>
 *   <li>{@link movies.Person#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see movies.MoviesPackage#getPerson()
 * @model abstract="true"
 * @generated
 */
public interface Person extends EObject {
	/**
	 * Returns the value of the '<em><b>Movies</b></em>' reference list.
	 * The list contents are of type {@link movies.Movie}.
	 * It is bidirectional and its opposite is '{@link movies.Movie#getPersons <em>Persons</em>}'.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Movies</em>' reference list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Movies</em>' reference list.
	 * @see movies.MoviesPackage#getPerson_Movies()
	 * @see movies.Movie#getPersons
	 * @model opposite="persons"
	 * @generated
	 */
	EList<Movie> getMovies();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Name</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see movies.MoviesPackage#getPerson_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link movies.Person#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model ordered="false"
	 *        annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='self.movies-&gt;collect(persons)-&gt;flatten()-&gt;asSet()'"
	 * @generated
	 */
	EList<Person> coactors();

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='self.movies-&gt;excludingAll(p.movies)-&gt;size() &lt;= (self.movies-&gt;size() - 3)'"
	 * @generated
	 */
	Boolean areCouple(Person p);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='self.name &lt; co.name and co.movies-&gt;size() &gt;= 3 and self.areCouple(co)'"
	 * @generated
	 */
	Boolean areCoupleCoactors(Person co);

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @model annotation="http://www.eclipse.org/emf/2002/Ecore/OCL/Pivot body='Person.allInstances()-&gt;select(a | a.coactors()-&gt;exists(areCoupleCoactors(a)))-&gt;size()'"
	 * @generated
	 */
	BigInteger QUERY();

} // Person

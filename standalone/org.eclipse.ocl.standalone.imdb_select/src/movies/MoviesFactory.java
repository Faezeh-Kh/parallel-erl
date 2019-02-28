/**
 */
package movies;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see movies.MoviesPackage
 * @generated
 */
public interface MoviesFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	MoviesFactory eINSTANCE = movies.impl.MoviesFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Actor</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Actor</em>'.
	 * @generated
	 */
	Actor createActor();

	/**
	 * Returns a new object of class '<em>Actress</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Actress</em>'.
	 * @generated
	 */
	Actress createActress();

	/**
	 * Returns a new object of class '<em>Couple</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Couple</em>'.
	 * @generated
	 */
	Couple createCouple();

	/**
	 * Returns a new object of class '<em>Movie</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Movie</em>'.
	 * @generated
	 */
	Movie createMovie();

	/**
	 * Returns a new object of class '<em>Clique</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Clique</em>'.
	 * @generated
	 */
	Clique createClique();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	MoviesPackage getMoviesPackage();

} //MoviesFactory

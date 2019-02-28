/**
 */
package movies.impl;

import java.util.Collection;

import movies.Group;
import movies.Movie;
import movies.MoviesPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Group</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link movies.impl.GroupImpl#getCommonMovies <em>Common Movies</em>}</li>
 *   <li>{@link movies.impl.GroupImpl#getAvgRating <em>Avg Rating</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class GroupImpl extends MinimalEObjectImpl.Container implements Group {
	/**
	 * The cached value of the '{@link #getCommonMovies() <em>Common Movies</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCommonMovies()
	 * @generated
	 * @ordered
	 */
	protected EList<Movie> commonMovies;

	/**
	 * The default value of the '{@link #getAvgRating() <em>Avg Rating</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAvgRating()
	 * @generated
	 * @ordered
	 */
	protected static final double AVG_RATING_EDEFAULT = 0.0;

	/**
	 * The cached value of the '{@link #getAvgRating() <em>Avg Rating</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAvgRating()
	 * @generated
	 * @ordered
	 */
	protected double avgRating = AVG_RATING_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GroupImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MoviesPackage.Literals.GROUP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Movie> getCommonMovies() {
		if (commonMovies == null) {
			commonMovies = new EObjectResolvingEList<Movie>(Movie.class, this, MoviesPackage.GROUP__COMMON_MOVIES);
		}
		return commonMovies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public double getAvgRating() {
		return avgRating;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAvgRating(double newAvgRating) {
		double oldAvgRating = avgRating;
		avgRating = newAvgRating;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MoviesPackage.GROUP__AVG_RATING, oldAvgRating, avgRating));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case MoviesPackage.GROUP__COMMON_MOVIES:
				return getCommonMovies();
			case MoviesPackage.GROUP__AVG_RATING:
				return getAvgRating();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case MoviesPackage.GROUP__COMMON_MOVIES:
				getCommonMovies().clear();
				getCommonMovies().addAll((Collection<? extends Movie>)newValue);
				return;
			case MoviesPackage.GROUP__AVG_RATING:
				setAvgRating((Double)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case MoviesPackage.GROUP__COMMON_MOVIES:
				getCommonMovies().clear();
				return;
			case MoviesPackage.GROUP__AVG_RATING:
				setAvgRating(AVG_RATING_EDEFAULT);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case MoviesPackage.GROUP__COMMON_MOVIES:
				return commonMovies != null && !commonMovies.isEmpty();
			case MoviesPackage.GROUP__AVG_RATING:
				return avgRating != AVG_RATING_EDEFAULT;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (avgRating: ");
		result.append(avgRating);
		result.append(')');
		return result.toString();
	}

} //GroupImpl

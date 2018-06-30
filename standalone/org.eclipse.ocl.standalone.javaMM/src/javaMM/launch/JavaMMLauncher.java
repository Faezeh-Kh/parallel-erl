package javaMM.launch;

import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.ocl.standalone.*;
import javaMM.JavaMMPackage;
import javaMM.util.JavaMMValidator;

public class JavaMMLauncher {
	public static void main(String[] args) {
		new StandaloneOCL(
			StandaloneOCLBuilder.newCompiledInstance(JavaMMPackage.eINSTANCE, JavaMMValidator.INSTANCE, args)
		) {
			@Override
			protected ConstraintDiagnostician createDiagnostician(Resource modelImpl) {
				return new ConstraintDiagnostician(modelImpl, false);
			}
		}
		.run();
	}
}

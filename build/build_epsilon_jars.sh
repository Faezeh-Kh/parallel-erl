unzip -o DistributedEVL.jar -d DistributedEVL
mv DistributedEVL.jar ../bin/DistributedEVL.jar
rm -r ERL/org/eclipse/epsilon/*
cp -r DistributedEVL/org/eclipse/epsilon/* ERL/org/eclipse/epsilon/
rm -r EVL
mkdir -p EVL/org/eclipse/epsilon/evl
mv ERL/org/eclipse/epsilon/evl/ EVL/org/eclipse/epsilon
unzip -o OCL.jar -d OCL
rm OCL.jar
rm -r OCL_lib
rm -r OCL/org/eclipse/epsilon/common/module
rm -r OCL/org/eclipse/epsilon/common/parse
rm -r OCL/org/apache
./build_epsilon_jar.sh ERL
./build_epsilon_jar.sh EVL
./build_epsilon_jar.sh DistributedEVL
./build_epsilon_jar.sh OCL
./build_epsilon_jar.sh OCL_java_simple

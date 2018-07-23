package sequential.reflection;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

@Deprecated
public class CallAMethod {

    public static Object parseFromString(String str, Class theClass) {
        if (theClass == Boolean.class) {
            return Boolean.parseBoolean(str); // TODO: add more types
        }
        return null;
    }

    /**
     * Given user input parameters (all string type), load the specified class and call the specified method
     * @param args a list of strings
     */
    public static void callMethodFromClass(String[] args)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException, MalformedURLException {
        String classURL = args[0];
        String className = args[1];
        int numConstructorArgs = Integer.parseInt(args[2]);
        Class[] constructorArgTypes = new Class[numConstructorArgs];
        Object[] constructorArgValues = new Object[numConstructorArgs];
        for (int i = 0; i < numConstructorArgs; i++) {
            constructorArgTypes[i] = Class.forName(args[3 + 2 * i]);
            constructorArgValues[i] = parseFromString(args[4 + 2 * i], constructorArgTypes[i]);
        }
        int curIndex = 3 + 2*numConstructorArgs;

        // obtain the method in the class
        String methodName = args[curIndex++];
        int numArgs = Integer.parseInt(args[curIndex++]);
        Class[] methodArgTypes = new Class[numArgs];
        Object[] methodArgValues = new Object[numArgs];
        for (int i = 0; i < numArgs; i++) {
            methodArgTypes[i] = Class.forName(args[curIndex++]);
            methodArgValues[i] = args[curIndex++];
        }

        ObjectAndMethod oam = loadClassAndMethod(classURL, className, constructorArgTypes, constructorArgValues,
                methodName, methodArgTypes);

        Object returnValue = callMethod(oam, methodArgValues);

        List<String> results = (List<String>) returnValue;
        for (String str : results) {
            System.out.println(str);
        }
    }

    /**
     * Given user input parameters (all string type), load the specified class and call the specified method.
     * (Notice the class file or jar file is loaded first, as the method may require parameters of
     *  types defined in the loaded file. )
     * @param args a list of strings
     */
    public static void evokeMethodFromClass(String[] args) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException, IOException {
        // load class from .class or .jar file
        String classURL = args[0];
        String className = args[1];
        URL[] classLoaderUrls = new URL[]{new URL(classURL)};
        ClassLoader cl = new URLClassLoader(classLoaderUrls);
        Class<?> aClass = cl.loadClass(className);

//        // test begins
//        JarFile jarFile = new JarFile("./plugins/ij.jar");
//        Enumeration<JarEntry> e = jarFile.entries();
//        URL[] classLoaderUrls = new URL[]{new File("./plugins/ij.jar").toURL()};
//        while (e.hasMoreElements()) {
//            JarEntry je = e.nextElement();
//            if(je.isDirectory() || !je.getName().endsWith(".class")){
//                continue;
//            }
//            // -6 because of .class
//            String clname = je.getName().substring(0,je.getName().length()-6);
//            clname = clname.replace('/', '.');
//            Class c = cl.loadClass(clname);
//
//        }
//        // test ends


        // create an instance of the class
        int numConstructorArgs = Integer.parseInt(args[2]);
        Class[] constructorArgTypes = new Class[numConstructorArgs];
        Object[] constructorArgValues = new Object[numConstructorArgs];
        for (int i = 0; i < numConstructorArgs; i++) {
            constructorArgTypes[i] = Class.forName(args[3 + 2 * i]);
            constructorArgValues[i] = parseFromString(args[4 + 2 * i], constructorArgTypes[i]);
        }
        Constructor<?> constructor = aClass.getConstructor(constructorArgTypes);
        Object obj = constructor.newInstance(constructorArgValues);

        int curIndex = 3 + 2*numConstructorArgs;

        // obtain the method in the class
        String methodName = args[curIndex++];
        int numArgs = Integer.parseInt(args[curIndex++]);
        Class[] methodArgTypes = new Class[numArgs];
        Object[] methodArgValues = new Object[numArgs];
        for (int i = 0; i < numArgs; i++) {
            String argType = args[curIndex++];
            if (argType.equals("ij.ImagePlus")) {
                methodArgTypes[i] = cl.loadClass(argType); // or Class.forName(args[curIndex++], true, cl);
            } else {
                methodArgTypes[i] = parseClassFromString(argType);
            }
            methodArgValues[i] = args[curIndex++];
        }
        Method method = aClass.getMethod(methodName, methodArgTypes);

        ObjectAndMethod oam = new ObjectAndMethod(obj, method);

        Object returnValue = callMethod(oam, methodArgValues);
    }

    public static Class parseClassFromString(String str) throws ClassNotFoundException {
        if (str.equals("java.lang.double"))
            return double.class;
        else if (str.equals("java.lang.int"))
            return int.class;
        else
            return Class.forName(str);
    }

    public static class ObjectAndMethod {
        public final Object obj;
        public final Method method;

        public ObjectAndMethod(Object _o, Method _m) {
            this.obj = _o;
            this.method = _m;
        }
    }

    public static ObjectAndMethod loadClassAndMethod(String classURL, String className,
                                    Class[] constructorArgTypes, Object[] constructorArgValues,
                                    String methodName, Class[] methodArgTypes)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        URL[] classLoaderUrls = new URL[]{new URL(classURL)};
        ClassLoader cl = new URLClassLoader(classLoaderUrls);
        Class<?> aClass = cl.loadClass(className);
        Constructor<?> constructor = aClass.getConstructor(constructorArgTypes);
        Object obj = constructor.newInstance(constructorArgValues);
        Method method = aClass.getMethod(methodName, methodArgTypes);
        return new ObjectAndMethod(obj, method);
    }

    public static Object callMethod(ObjectAndMethod oam, Object[] methodArgValues)
            throws InvocationTargetException, IllegalAccessException {
        return oam.method.invoke(oam.obj, methodArgValues);
    }

    public static Object callMethodFromClass(String classURL, String className,
                                             Class[] constructorArgTypes, Object[] constructorArgValues,
                                             String methodName, Class[] methodArgTypes, Object[] methodArgValues)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        // Load the target class
        URL[] classLoaderUrls = new URL[]{new URL(classURL)};
        ClassLoader cl = new URLClassLoader(classLoaderUrls);
        Class<?> aClass = cl.loadClass(className);
        Constructor<?> constructor = aClass.getConstructor(constructorArgTypes);

        // evoke the method
        Method method = aClass.getMethod(methodName, methodArgTypes);
        return method.invoke(constructor.newInstance(constructorArgValues), methodArgValues);
    }

    public static void test2(String classURL, String className,
                             Class[] constructorArgTypes, Object[] constructorArgValues,
                             String methodName, Class[] methodArgTypes, Object[] methodArgValues)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException, MalformedURLException, ClassNotFoundException {
//        // load the class
//        String classURL = "file:///Users/RundongL/MyWorkStack/repos/ProjectsCode/MapReduceReflection" +
//                "/target/classes/sequential/plugins";
//        String className = "sequential.plugins.WordCount";
//        Class[] constructorArgTypes = new Class[]{boolean.class};
//        Object[] constructorArgValues = new Object[]{true};
//
//        // obtain the method in the class
//        String methodName = "countWords";
//        Class[] methodArgTypes = new Class[]{String.class};

        ObjectAndMethod oam = loadClassAndMethod(classURL, className, constructorArgTypes, constructorArgValues,
                methodName, methodArgTypes);

//        Object[] methodArgValues = new Object[]{"abacus vmware linux ledare led abacus Linux Linux"};

        Object returnValue = callMethod(oam, methodArgValues);

        List<String> results = (List<String>) returnValue;
        for (String str : results) {
            System.out.println(str);
        }
    }

    public static void test1() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException, MalformedURLException, ClassNotFoundException {
//        // Getting the jar URL which contains target class
//        URL[] classLoaderUrls = new URL[]{new URL("file:///Users/RundongL/MyWorkStack/repos/" +
//                "ProjectsCode/MapReduceReflection/target/classes/sequential/plugins")};
////          // Create a new URLClassLoader: does this work?
////          URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);
//
//        // Load the target class
//        ClassLoader cl = new URLClassLoader(classLoaderUrls);
//        Class<?> aClass = cl.loadClass("sequential.plugins.WordCount");
//        String className = aClass.getName();
//        System.out.println("class name is " + className);
////        String fieldName = aClass.getField("caseSensitive").getName();
////        System.out.println("field name is " + fieldName);
//
//        Constructor<?> constructor = aClass.getConstructor(boolean.class);
//        Method method = aClass.getMethod("countWordsInFile", String.class, String.class);

        // load the class
        String classURL = "file:///Users/RundongL/MyWorkStack/repos/ProjectsCode/MapReduceReflection" +
                "/target/classes/sequential/plugins";
        String className = "sequential.plugins.WordCount";
        Class[] constructorArgTypes = new Class[]{boolean.class};
        Object[] constructorArgValues = new Object[]{true};

        // call the method in the class
        String methodName = "countWordsInFile";
        Class[] methodArgTypes = new Class[]{String.class, String.class};
        Object[] methodArgValues = new Object[]{
                "./testData/word_count/Sample EMR Makefile", "./testData/word_count/countsCaseSensitive"};

        Object returnValue = callMethodFromClass(classURL, className, constructorArgTypes, constructorArgValues,
                methodName, methodArgTypes, methodArgValues);
        System.out.println("returnValue = " + returnValue);
        if (Boolean.FALSE.equals(returnValue)) {
            System.err.println(methodName + " does not complete correctly!");
        }
    }

    public static void main(String[] args) throws Exception {
        callMethodFromClass(args);
//        evokeMethodFromClass(args);
    }
}

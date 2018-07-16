package sequential.reflection;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Utils {

    public static Class<?> getClass(String classURL, String className)
            throws MalformedURLException, ClassNotFoundException {
        URL[] classLoaderUrls = new URL[]{new URL(classURL)};
        ClassLoader cl = new URLClassLoader(classLoaderUrls);
        return cl.loadClass(className);
    }

    public static Object getInstance(Class<?> aClass, Class[] constructorArgTypes, Object[] constructorArgValues)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<?> constructor = aClass.getConstructor(constructorArgTypes);
        return constructor.newInstance(constructorArgValues);
    }

    /**
     *
     */
    public static Method getMethod(Class<?> aClass, String methodName, Class[] methodArgTypes) {
        try {
            return aClass.getMethod(methodName,methodArgTypes);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * The {@code aClass} argument may be ignored, if the {@code method} is static.
     */
    public static Object callMethod(Object obj, Method method, Object[] methodArgValues) {
        try {
            return method.invoke(obj, methodArgValues);
        } catch (IllegalAccessException e) {
            System.err.println("in callMethod()");
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

}

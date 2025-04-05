package org.duckdb.test;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.function.Function;
import org.duckdb.test.Thrower;

public class Assertions {
    public static void assertTrue(boolean val) throws Exception {
        assertTrue(val, null);
    }

    public static void assertTrue(boolean val, String message) throws Exception {
        if (!val) {
            throw new Exception(message);
        }
    }

    public static void assertFalse(boolean val) throws Exception {
        assertTrue(!val);
    }

    public static void assertFalse(boolean val, String message) throws Exception {
        assertTrue(!val, message);
    }

    public static void assertEquals(Object actual, Object expected) throws Exception {
        assertEquals(actual, expected, "");
    }

    public static void assertEquals(Object actual, Object expected, String label) throws Exception {
        Function<Object, String> getClass = (Object a) -> a == null ? "null" : a.getClass().toString();

        String message = label.isEmpty() ? "" : label + ": ";
        message += String.format("\"%s\" (of %s) should equal \"%s\" (of %s)", actual, getClass.apply(actual), expected,
                                 getClass.apply(expected));
        // Note this will fail for arrays, which do not implement .equals and so fall back to reference equality checks.
        assertTrue(Objects.equals(actual, expected), message);
    }

    public static void assertEquals(byte[] actual, byte[] expected, String message) throws Exception {
        assertTrue(Arrays.equals(actual, expected), message);
    }

    public static <T> void assertListsEqual(List<T> actual, List<T> expected) throws Exception {
        assertListsEqual(actual, expected, "");
    }

    public static <T> void assertListsEqual(List<T> actual, List<T> expected, String label) throws Exception {
        assertEquals(actual.size(), expected.size());

        ListIterator<T> itera = actual.listIterator();
        ListIterator<T> itere = expected.listIterator();

        while (itera.hasNext()) {
            assertEquals(itera.next(), itere.next(), label);
        }
    }

    public static void assertNotNull(Object a) throws Exception {
        assertFalse(a == null);
    }

    public static void assertNull(Object a) throws Exception {
        assertEquals(a, null);
    }

    public static void assertEquals(double a, double b, double epsilon) throws Exception {
        assertTrue(Math.abs(a - b) < epsilon);
    }

    public static void fail() {
        fail(null);
    }

    public static void fail(String s) {
        throw new RuntimeException(s);
    }

    public static <T extends Throwable> String assertThrows(Thrower thrower, Class<T> exception) throws Exception {
        return assertThrows(exception, thrower).getMessage();
    }

    private static <T extends Throwable> Throwable assertThrows(Class<T> exception, Thrower thrower) throws Exception {
        try {
            thrower.run();
        } catch (Throwable e) {
            if (!exception.isAssignableFrom(e.getClass())) {
                throw new Exception("Expected to throw " + exception.getName() + ", but threw " +
                                    e.getClass().getName());
            }
            return e;
        }
        throw new Exception("Expected to throw " + exception.getName() + ", but no exception thrown");
    }

    // Asserts we are either throwing the correct exception, or not throwing at all
    public static <T extends Throwable> boolean assertThrowsMaybe(Thrower thrower, Class<T> exception)
        throws Exception {
        try {
            thrower.run();
            return true;
        } catch (Throwable e) {
            if (e.getClass().equals(exception)) {
                return true;
            } else {
                throw new Exception("Unexpected exception: " + e.getClass().getName());
            }
        }
    }
}

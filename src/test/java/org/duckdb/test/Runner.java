package org.duckdb.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Runner {
    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static int runTests(String[] args, Class<?>... testClasses) {
        // Woo I can do reflection too, take this, JUnit!
        List<Method> methods = Arrays.stream(testClasses)
                                   .flatMap(clazz -> Arrays.stream(clazz.getMethods()))
                                   .sorted(Comparator.comparing(Method::getName))
                                   .collect(Collectors.toList());

        String specific_test = null;
        boolean quick_run = false;
        if (args.length >= 1) {
            if ("quick".equals(args[0])) {
                quick_run = true;
            } else {
                specific_test = args[0];
            }
        }

        boolean anySucceeded = false;
        boolean anyFailed = false;
        for (Method m : methods) {
            if (m.getName().startsWith("test_")) {
                if (quick_run && m.getName().startsWith("test_lots_")) {
                    continue;
                }
                if (specific_test != null && !m.getName().contains(specific_test)) {
                    continue;
                }
                System.out.print(m.getDeclaringClass().getSimpleName() + "#" + m.getName() + " ");

                LocalDateTime start = LocalDateTime.now();
                try {
                    m.invoke(null);
                    System.out.println("success in " + Duration.between(start, LocalDateTime.now()).getSeconds() +
                                       " seconds");
                } catch (Throwable t) {
                    if (t instanceof InvocationTargetException) {
                        t = t.getCause();
                    }
                    System.out.println("failed with " + t);
                    t.printStackTrace(System.out);
                    anyFailed = true;
                }
                anySucceeded = true;
            }
        }
        if (!anySucceeded) {
            System.out.println("No tests found that match " + specific_test);
            return 1;
        }
        System.out.println(anyFailed ? "FAILED" : "OK");

        return anyFailed ? 1 : 0;
    }
}

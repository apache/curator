# ZooKeeper 3.4 Compatibility

Apache Curator 4.0 adds best-effort backward compatibility for ZooKeeper 3.4 ensembles and the ZooKeeper 3.4 library/JAR. This module, `curator-test-zk34`, exists to run the standard Curator tests using ZooKeeper 3.4. It does this via Maven. In the curator-test-zk34 pom.xml:

- The Curator modules framework and recipes libraries are included - both main and test JARs - but the ZooKeeper dependency is excluded (otherwise ZooKeeper 3.5.x would be brought in)
- The curator-test module is included but as version 2.12.0 which brings in ZooKeeper 3.4.8
- The maven-surefire-plugin is configured to run the framework and recipes tests
- The current version of the curator-test module includes new methods that didn't exist in version 2.12.0 in `Timing.java` and `KillSession.java`. Therefore, these classes are now soft-deprecated, reverted to their original implementations and there are new classes with the new methods: `Timing2.java` and `KillSession2.java`
- A new test base class `CuratorTestBase` is started. Over time more common stuff should go in here but, for now, this defines a TestNG listener, Zk35MethodInterceptor, that allows for tests that are ZooKeeper 3.5 only to be marked by `@Test(groups = Zk35MethodInterceptor.zk35Group)`. These tests will not be run during the 3.4 compatibility check.
- curator-test-zk34 needs some of the new classes from curator-test. Rather than have copies of the classes the maven-resources-plugin is used to copy from curator-test to the generated sources dir of curator-test-zk34. All classes in `curator-test/src/main/java/org/apache/curator/test/compatibility` are copied.


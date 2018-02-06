package io.snappydata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.function.Supplier;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.shared.ClientResolverUtils;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;

public class TestClass {

  private final HashMap<String, Supplier<DataSerializable>> registeredTypes =
      new HashMap<>();

  private void register(String key, Supplier<DataSerializable> f) {
    registeredTypes.put(key, f);
  }

  private void invoke(String key) throws IOException, ClassNotFoundException {
    DataSerializable v = registeredTypes.get(key).get();
    v.fromData(null);
    System.out.println(v);
  }

  public static void main(String[] args) throws Exception {
    /*
    TestClass test = new TestClass();
    System.out.println("Registering");
    test.register("T1", T1::new);
    System.out.println("Registering Child");
    test.register("Child", T1.Child::new);
    System.out.println("Invoking");
    test.invoke("T1");
    System.out.println("Invoking");
    test.invoke("T1");
    System.out.println("Invoking Child");
    test.invoke("Child");
    System.out.println("Invoking Child");
    test.invoke("Child");
    */
    byte[] bytes = ClientResolverUtils.class.getName().getBytes(StandardCharsets.UTF_8);
    final int warmups = 100;
    final int measure = 1000;

    long total = 0;
    for (int i = 0; i < warmups; i++) {
      total += ClientResolverUtils.addBytesToHash(bytes, 42);
    }
    System.out.println("1: Total=" + total);

    total = 0;
    long start = System.nanoTime();
    for (int i = 0; i < measure; i++) {
      total += ClientResolverUtils.addBytesToHash(bytes, 42);
    }
    long end = System.nanoTime();
    System.out.println("1: Time taken = " + (end - start) + "ns, total=" + total);

    total = 0;
    for (int i = 0; i < warmups; i++) {
      total += Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length, 42);
    }
    System.out.println("2: Total=" + total);

    total = 0;
    start = System.nanoTime();
    for (int i = 0; i < measure; i++) {
      total += Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length, 42);
    }
    end = System.nanoTime();
    System.out.println("2: Time taken = " + (end - start) + "ns, total=" + total);
  }
}

final class T1 implements DataSerializable {

  static {
    System.out.println("T1.cinit");
  }

  static final class Child implements DataSerializable {

    static {
      System.out.println("Child.cinit");
    }

    @Override
    public void toData(DataOutput out) throws IOException {
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }
}

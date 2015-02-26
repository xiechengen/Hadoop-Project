package org.apache.hive.cs516;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator; import org.apache.hadoop.io.DoubleWritable;
public class App extends UDAF {
public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
private DoubleWritable result;
public void init() { result = null;
}
public boolean iterate(DoubleWritable value) { if (value == null) {
return true; }
if (result == null) {
result = new DoubleWritable(value.get());
} else {
result.set(Math.max(result.get(), value.get()));
}
return true; }
public DoubleWritable terminatePartial() { return result;
}
public boolean merge(DoubleWritable other) { return iterate(other);
}
public DoubleWritable terminate() { return result;
} }
}

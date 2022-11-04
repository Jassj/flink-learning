package udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SubstringFunction extends ScalarFunction {

    private boolean endInclusive;

    public SubstringFunction(boolean endInclusive) {
        this.endInclusive = endInclusive;
    }

    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, endInclusive ? end + 1 : end);
    }

}
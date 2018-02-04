package simpledb.aggregator;


import simpledb.exception.ParsingException;

/**
 * Aggregate Functions
 *
 * @Author jason leaster
 */
public enum AggregateFunc {

    MIN("MIN"),

    MAX("MAX"),

    SUM("SUM"),

    AVG("AVG"),

    COUNT("COUNT"),

    /**
     * SUM_COUNT: compute sum and count simultaneously, will be
     * needed to compute distributed avg in lab7.
     */
    SUM_COUNT("SUM_COUNT"),

    /**
     * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
     * will be used to compute distributed avg in lab7.
     */
    SC_AVG("SC_AVG");

    private String name;

    AggregateFunc(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static AggregateFunc getFuncByName(String name) throws ParsingException {
        if (name == null || name.isEmpty()){
            throw new ParsingException("Empty String!");
        }

        name = name.toUpperCase();
        return AggregateFunc.valueOf(name);
    }

    /**
     * Interface to access operations by a string containing an integer
     * index for command-line convenience.
     *
     * @param s a string containing a valid integer AggregateFunc index
     */
    public static AggregateFunc getOp(String s) {
        return getOp(Integer.parseInt(s));
    }

    /**
     * Interface to access operations by integer value for command-line
     * convenience.
     *
     * @param i a valid integer AggregateFunc index
     */
    public static AggregateFunc getOp(int i) {
        return values()[i];
    }

    @Override
    public String toString() {
        return this.getName().toUpperCase();
    }
}

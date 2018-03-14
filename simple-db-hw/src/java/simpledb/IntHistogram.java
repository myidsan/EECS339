package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private int num_buckets;
    private int step;
    private int total_values;
    private int min_value;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.num_buckets = buckets;
        this.step = (int) Math.ceil((double) (max - min + 1)/buckets);
        this.total_values = 0;
        this.min_value = min;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        // div should be casted back to an int representing array index (bucket num)
        int bucket = (v - this.min_value)/this.step;
        this.buckets[bucket]++;
        this.total_values++;
    }

    // added api
    // returns the bucket of the given value v
    private int findBucket(int v)
    {
        int bucket = (v - this.min_value)/this.step;
        if (bucket < 0) {
            bucket = -1;
        }        	
        if (bucket >= this.num_buckets) {
            bucket = this.num_buckets;
        }        	
        return bucket;
    }
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        switch(op) {
            case EQUALS:
            case LIKE:
                return estimateSelectivityEquals(v);
            case GREATER_THAN:
            case LESS_THAN:
                return estimateSelectivityInequality(op,v);
            case LESS_THAN_OR_EQ:
                return (estimateSelectivityEquals(v) + estimateSelectivityInequality(Predicate.Op.LESS_THAN,v));
            case GREATER_THAN_OR_EQ:
                return (estimateSelectivityEquals(v) + estimateSelectivityInequality(Predicate.Op.GREATER_THAN,v));
            case NOT_EQUALS:
                return 1.0 - estimateSelectivityEquals(v);
            default:
                return -1.0; 
        }
    }

    // added api for equality
    private double estimateSelectivityEquals(int v) {
        int bucket = findBucket(v);
        // out on index cases
        if (bucket < 0) {
            return 0.0;
        }        	
        if (bucket >= this.num_buckets) {
            return 0.0;
        }        	
        // estimated cost of finding the v
        int height = this.buckets[bucket];
        return (double) ((double) height/this.step)/this.total_values;
    }

    // added api for inequality
    private double estimateSelectivityInequality(Predicate.Op op, int v) {
        int bucket = findBucket(v);
        int bucket_f, b_right, b_left, height;
        
        // v is less than the min value
        // so greater than should return everything (1.0)
        // and less than should return 0.0
        if (bucket < 0) {
        	b_right = 0;
        	b_left = -1;
            bucket_f = 0;
            height = 0;
        }
        // v is greater than the max value
        // so greater than should return 0.0
        // and less than should return 1.0
        else if (bucket >= this.num_buckets) {
        	b_right = this.num_buckets;
        	b_left = this.num_buckets-1; 
            bucket_f = 0;
            height = 0;
        } else {
        	b_right = bucket+1;
        	b_left = bucket-1;
            bucket_f = -1;
            height = this.buckets[bucket];
        }
        double selectivity = 0.0;
        switch(op) {
            case GREATER_THAN:
                if (bucket_f == -1) {
                    bucket_f = ((b_right*this.step)+this.min_value-v)/this.step;
                }
                selectivity = (height * bucket_f) / this.total_values;
            	if (b_right >= this.num_buckets)
            		return selectivity/this.total_values;
            	for (int i = b_right; i < this.num_buckets; i++)
            	{
            		selectivity += this.buckets[i];
            	}
            	return selectivity/this.total_values;
            case LESS_THAN:
                if (bucket_f == -1) {
                    bucket_f = (v-(b_left*this.step)+this.min_value)/this.step;
                }
                selectivity = (height * bucket_f) / this.total_values;
            	if (b_left < 0)
            		return selectivity/this.total_values;
            	for (int i = b_left; i >= 0; i--)
            	{
            		selectivity += this.buckets[i];
            	}
            	return selectivity/this.total_values;
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String s = "";
    	for (int i = 0; i < this.num_buckets; i++)
    	{
    		s += "bucket " + i + ": ";
    		for (int j = 0; j < this.buckets[i]; j++)
    		{
    			s += "|";
    		}
    		s += "\n";
    	}
        // some code goes here
        return s;
    }
}
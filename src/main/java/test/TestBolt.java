/**
 * 
 */
package test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * This bolt simply consumes the stream, this is useful for testing where you want
 * the tuple to get marked as processed, but you have a dangling output.
 * @author redman
 */
public class TestBolt extends BaseBasicBolt {

    /** nothing to see here. */
    private static final long serialVersionUID = 1L;
        
    /** number times to repeat the thing. */
    private int duration = 40;
    
    /** we just compute and store here. */
    double sum = 0;
    
    /**
     * @param duration number of ticks to consume in this bolt.
     */
    public TestBolt(int duration) {
        super();
        this.duration = duration;
    }

    /**
     * No output fields.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Double[] numbers = (Double[])input.getValue(0);
        long start = System.currentTimeMillis();
        for (int i = 0 ; (System.currentTimeMillis() - start) < duration ; i++) {
            for (Double d : numbers) {
                sum = Math.log(d * i);
            }
        }
    }
}

/**
 * 
 */
package test;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This spout simply spits out random integers in an array, this is used for testing.
 * @author redman
 */
public class TestSpout extends BaseRichSpout {

    /** default. */
    private static final long serialVersionUID = 1L;

    /** this is the number of integers to produce */
    private int size = 300;

    /** random number generator. */
    final private Random random = new Random();

    /** the collector. */
    private SpoutOutputCollector collector = null;

    /** counts messages. */
    private long messageId = 0;

    /** the reporting interval. */
    final long RI = 60000;

    /** the last report. */
    private long last = System.currentTimeMillis() + RI;

    /** the number of tuples outstanding. */
    private long failed = 0;

    /** the number of tuples outstanding. */
    private long succeeded = 0;

    /** the number of unacked tuples laying around. */
    private HashSet<Long> unackedTuples = null;
    
    /** if this is set anchor the tuple stream. */
    private boolean anchorTupleStream = false;

    /**
     * @param num number of integers to produce. 
     * @param anchorTupleStream if true, anchor the tuple stream for replays.
     */
    public TestSpout(int num, boolean anchorTupleStream) {
        this.size = num;
        this.anchorTupleStream = anchorTupleStream;
    }

    /**
     * create the channel when we open the bolt.
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.unackedTuples = new HashSet<Long>();
    }

    /**
     * initialize an array of Doubles, push those downstream, reporting periodically. Keep
     * track of counts over time, and last report.
     */
    @Override
    public void nextTuple() {
        Double[] values = new Double[size];
        for (int i = 0; i < size; i++) {
            values[i] = random.nextDouble();
        }
        if (anchorTupleStream) {
            this.collector.emit(new Values((Object) values), new Long(messageId));
        } else {
            this.collector.emit(new Values((Object) values));
        }
        this.messageId++;
        if (last < System.currentTimeMillis()) {
            System.out.println ("on message " + messageId + " good : " + succeeded + " bad : " + failed);
            last = System.currentTimeMillis() + RI;
        }
    }

    @Override
    public void ack(Object msgId) {
        synchronized (this) {
            unackedTuples.remove((Long) msgId);
            succeeded++;
        }
    }

    @Override
    public void fail(Object msgId) {
        synchronized (this) {
            unackedTuples.remove((Long) msgId);
            failed++;
        }
    }

    /**
     * Only output field is the double data. 
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("DOUBLE_DATA"));
    }
}

/**
 * 
 */
package test;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates a storm issue where anchoring tuples for reliability breaks 
 * cross node parallelism. Within a node (the one with the spout), works fine, but we see
 * no activity on any other node.
 * @author redman
 */
public class TestParallelismTopology {

    
    /**
     * Submit this to a remote cluster, since this is a test of Storms distribution 
     * capabilities.
     * @param args first argument is always the number of values to push, second is compute factor.
     * @throws Exception for any reason
     */
    public static void main(String[] args) throws Exception {
        int number_values = 100;       // size of the double array to chew on.
        int duration = 50;          // size of the computation across the numbers array
        boolean anchorStreams = false;   // true to anchor streams, this causes issues.
       
        // Used to build the topology
        System.out.println("Execute topology with tuples containing "+number_values+
            " doubles, counter running "+duration+"ms "+
                        (anchorStreams ? "with anchors" : "without anchors"));
        TopologyBuilder builder = new TopologyBuilder();

        // spout with only one thread
        builder.setSpout("spout", new TestSpout(number_values, anchorStreams), 1)
            .setMaxSpoutPending(1000);
        
        // number of iterations may come as an arg
        builder.setBolt("compute", new TestBolt(duration), 200)
		    .shuffleGrouping("spout");

        // new configuration
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);
        final String name = "DistributionTest";
        StormSubmitter.submitTopology(name, conf, builder.createTopology());
        System.out.println(name+" was deployed.");
    }
}

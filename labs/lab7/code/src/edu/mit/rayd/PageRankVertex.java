package edu.mit.rayd;

import org.apache.giraph.Algorithm;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;


@Algorithm(
    name="PageRank",
    description="Calculates the probability of ending up at a node by randomly following edges in a graph"
)
public class PageRankVertex extends Vertex<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

    final int MAX_NUM_SUPERSTEPS = 30;
    final double DAMPING_FACTOR = .85;

    private static final Logger log = Logger.getLogger(PageRankVertex.class);

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        // initialize value of node
        // in page rank this should be 1 / total number of nodes
        if (getSuperstep() == 0) {
            log.debug("Number of vertices: " + (getTotalNumVertices() + 1));
            setValue(new DoubleWritable(1d/(getTotalNumVertices() + 1)));
            log.debug("Initial PR value: " + getValue().get());
        }
        else {
            double value = 0;
            // process each incoming message
            for (DoubleWritable message : messages) {
                log.debug("[node " + getId().get() + "] Processing message with value: " + message.get());
                value += message.get();
            }
            // set the new value with damping factor
            setValue(new DoubleWritable(((1-DAMPING_FACTOR)/(getTotalNumVertices() + 1)) + (DAMPING_FACTOR * value)));
            log.debug("[node " + getId().get() + "] Set new node value: " + getValue().get());
        }

        // now if we want to continue the computation, send messages to all
        // neighboring nodes with fractional page rank value
        if(getSuperstep() < MAX_NUM_SUPERSTEPS) {
            sendMessageToAllEdges(new DoubleWritable(getValue().get() / getNumEdges()));
        }
        else {
            voteToHalt();
        }
    }
}

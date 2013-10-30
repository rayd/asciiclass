package edu.mit.rayd;

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

    final int MAX_NUM_SUPERSTEPS = 10;

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        // initialize value of node
        // in page rank this should be 1 / total number of nodes
        if (getSuperstep() == 0) {
           setValue(new DoubleWritable(1d/getTotalNumVertices()));
        }
        else {
            double value = 0;
            // process each incoming message
            for (DoubleWritable message : messages) {
                value += message.get();
            }
            // set the new value
            setValue(new DoubleWritable(value));

            // now if we want to continue the computation, send messages to all
            // neighboring nodes with fractional page rank value
            if(getSuperstep() < MAX_NUM_SUPERSTEPS) {
                sendMessageToAllEdges(new DoubleWritable(value / getNumEdges()));
            }
            else {
                voteToHalt();
            }
        }
    }
}

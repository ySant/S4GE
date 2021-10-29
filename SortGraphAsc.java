/*
 * SortGraphAsc.java
 * Sort the nodes of a WebGraph according to the out-degree,
 *   in ascending order.
 * 	 If two nodes have same degree, sort on the id.
 * Usage: java SortGraph basename
 *         where basename is the WebGraph basename
 * Output files: 
 *        basename-asc.graph
 *        basename-dsc.graph  -- for descending order
 *        basename-ascP   -- using Primitive
 * -
 * Version 1.00 - first version 
 *       - Jan, 2018 - Yudi Santoso
 * Version 1.10 - use Primitive from net.mintern 
 *       - 9 Feb, 2018 - Yudi Santoso
 */ 
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Arrays;
import net.mintern.primitive.Primitive;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.IncrementalImmutableSequentialGraph;

public class SortGraphAsc {

	String basename;
	ImmutableGraph G;
	int n;
    int[] deg;
	
	public SortGraphAsc(String basename) throws Exception {
		this.basename = basename;
		
		G = ImmutableGraph.loadMapped(basename);
		n = G.numNodes();
        deg = new int[n];
	}


	public void SortAndSave() throws Exception {
		
        final IncrementalImmutableSequentialGraph g = new IncrementalImmutableSequentialGraph();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<Void> future = executor.submit( new Callable<Void>() {
                public Void call() throws IOException {
                        BVGraph.store( g, basename+"-ascP" );
                        return null;
                }
        } );

        int[] idx = new int[n];   // new node labels
        for (int v=0; v<n; v++){
            deg[v] = G.outdegree(v);
            idx[v] = v;
        }
    // Sort ascending:
    //    Arrays.sort(idx, (o1,o2) -> Integer.compare(deg[o1], deg[o2]));
        Primitive.sort(idx, (o1,o2) -> Integer.compare(deg[o1], deg[o2]));
    // Sort descending:
    //    Arrays.sort(idx, (o1,o2) -> Integer.compare(-deg[o1], -deg[o2]));

        int[] vtx = new int[n];
        for(int i = 0; i < n; i++) vtx[idx[i]] = i;   // the new labels

        
		for(int v=0; v<n; v++) {
			int v_deg = G.outdegree(idx[v]);
			int[] v_succ = G.successorArray(idx[v]);  // translate this into new order
			
			int[] v_succ2 = new int[v_deg];
//            System.out.print(v + " : ");
			for(int i=0; i<v_deg; i++) {
//               System.out.print("   " + vtx[v_succ[i]]);
			   v_succ2[i]=vtx[v_succ[i]];
			}
//            System.out.println("");
			Arrays.sort(v_succ2);

			g.add(v_succ2, 0, v_deg);
		}

        g.add( IncrementalImmutableSequentialGraph.END_OF_GRAPH );
        future.get();
        executor.shutdown();
	}
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		
		String basename = args[0]; 
		
		SortGraphAsc t = new SortGraphAsc(basename);

		t.SortAndSave();
		
		System.out.println("Total time elapsed = " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}
}



/*
 * SortGraphAscBg.java
 * Sorts the nodes of a WebGraph according to the out-degree
 *   in ascending order. If two nodes have same degree, 
 *   sorts on the id. Relabels the nodes, then, filters the 
 *   result and saves only the neighbors with higher (new) 
 *   node id.
 * Note: this is a combination of SortGraphAsc.java
 *       and CreateBg.java
 * Usage: java SortGraphAscBg basename
 *         where basename is the WebGraph basename
 *         (on undirected graph this is the symmetrized one).
 * Output files: 
 *        basename-ascBg.graph
 * -
 * Requires: net.mintern.primitive and it.unimi.dsi.webgraph
 *           libraries.
 * -
 * Version 1.00 - first version 
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

public class SortGraphAscBg {

   String basename;
   ImmutableGraph G;
   int n;
   int[] deg;
	
   public SortGraphAscBg(String basename) throws Exception {
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
                        BVGraph.store( g, basename+"-ascBg" );
                        return null;
         }
      } );

      int[] idx = new int[n];   // new node labels
      for (int v=0; v<n; v++){
         deg[v] = G.outdegree(v);
         idx[v] = v;
      }
// Sort ascending:
      Primitive.sort(idx, (o1,o2) -> Integer.compare(deg[o1], deg[o2]));

      int[] vtx = new int[n];
      for(int i = 0; i < n; i++) vtx[idx[i]] = i;   // the new labels

        
      for(int v=0; v<n; v++) {
         if (v%1_000_000 == 0) System.out.println(v);
         int v_deg = G.outdegree(idx[v]);
         int[] v_succ = G.successorArray(idx[v]);  // translate this into new order
			
         int[] v_succ2 = new int[v_deg];    // use upper bound in size

         int j=0;
         for(int i=0; i<v_deg; i++) {
// Filter out the smaller neighbours 
            if (v<vtx[v_succ[i]]) {
			   v_succ2[j]=vtx[v_succ[i]];
               j++;
            }
         }

         int v_deg2B = j;
         int[] v_succ2B = new int[v_deg2B];    
         for(int i=0; i<v_deg2B; i++) {
			v_succ2B[i]=v_succ2[i];
//            System.out.print(v_succ2B[i] + "  ");
         }
//         System.out.println("");
         Arrays.sort(v_succ2B);   // webgraph compression requires this
//         for(int i=0; i<v_deg2B; i++) {
//            System.out.print(v_succ2B[i] + "  ");
//         }
//         System.out.println("");

         g.add(v_succ2B, 0, v_deg2B);  
      }

      g.add( IncrementalImmutableSequentialGraph.END_OF_GRAPH );
      future.get();
      executor.shutdown();
   }
	
   public static void main(String[] args) throws Exception {
      long startTime = System.currentTimeMillis();
		
      String basename = args[0]; 
		
      SortGraphAscBg t = new SortGraphAscBg(basename);

      t.SortAndSave();
		
      System.out.println("Total time elapsed = " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
   }
}



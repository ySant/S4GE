/*  
 * FourGraphlets.java
 * Enumerate three and four nodes graphlets through triangles and wedges. 
 *       - use Java8 parallel stream.
 * This is for simple undirected graphs. 
 * Dependency: 
 *       - WebGraph library.
 *       - java.util.stream.IntStream
 * Input: An undirected graph in webgraph format.
 *        We need both asc(P) (sorted) and 
 *                     ascBg (sorted & filtered for larger neighbours)
 *        the asc list contains all succesors.
 * Usage: java FourGraphlets basename
 *          the basename is without "-ascP" or "-ascBg"
 * Output: 
 *     List and/or counts of 3 and 4 node graphlets.
 * Algorithm: 
 *     Use Edge Iteration for finding triangles and wedges, 
 *         put inside parallel-stream.
 *     Use 3-set intersection to find corresponding 4-node graphlets.
 * Version 1.00 - first working version
 *      - Jul 10, 2019 - Yudi Santoso 
 * Version 1.10 - load both asc and ascBg input datasets
 *      - Jul 15, 2019 - Yudi Santoso 
 * Version 1.20 - some optimizations -- still not fast enough
 *      - Jul 20, 2019 - Yudi Santoso 
 * Version 1.21 - use long in Gcounts
 *      - Jul 20, 2019 - Yudi Santoso 
 * Version 1.30 - revision based on star test set
 *      - Jun 21, 2020
 *
 */
 
import it.unimi.dsi.webgraph.ImmutableGraph;
import java.util.stream.IntStream;
import java.util.Arrays;

public class FourGraphlets {

    String basename;
    ImmutableGraph G;
    ImmutableGraph GBg;
    int n;
    int nBg;
    long m;
    long mBg;
    int maxdeg;
    int maxdegBg;
	
    public FourGraphlets(String basename) throws Exception {
        this.basename = basename;
		
//        G = ImmutableGraph.load(basename); // for faster run, but limited by the size of the graph
        G = ImmutableGraph.loadMapped(basename + "-ascP");
        GBg = ImmutableGraph.loadMapped(basename + "-ascBg");
        n = G.numNodes();
        nBg = GBg.numNodes();
		
    // This pass is very fast as it does not read the neighbors of vertices. 
        maxdeg = 0; m = 0; maxdegBg = 0; mBg = 0;
        for(int v=0; v<n; v++) {
            int v_deg = G.outdegree(v);
            int v_Bgdeg = GBg.outdegree(v);
            m += v_deg;
            mBg += v_Bgdeg;
            if(v_deg > maxdeg)
                maxdeg = v_deg;
            if(v_Bgdeg > maxdegBg)
                maxdegBg = v_Bgdeg;
        }
        System.out.println("n=" + n + ", m=" + m + ", maxdeg=" + maxdeg);
        System.out.println("nBg=" + nBg + ", mBg=" + mBg + ", maxdegBg=" + maxdegBg);
    }

	
    public void compute() throws Exception {
 
        GCounts tot_counts = IntStream.range(0,n).parallel().mapToObj(u -> {
//        GCounts tot_counts = IntStream.range(0,n).mapToObj(u -> {
//            if(u%1_000_000 == 0) System.out.println(u);
            if(u%100_000 == 0) System.out.println(u);
//            if(u%1_000 == 0) System.out.println(u);
//            System.out.println("Node: " + u);    // for checking
            ImmutableGraph H = G.copy();
            ImmutableGraph HBg = GBg.copy();
            int[] u_Bgneighbors = HBg.successorArray(u);
            int u_Bgdeg = HBg.outdegree(u);
            int[] u_neighbors = H.successorArray(u);
            int u_deg = H.outdegree(u);
            GCounts uv_counts = IntStream.range(0,u_Bgdeg).mapToObj(iv -> {
                int v = u_Bgneighbors[iv];    // only bigger neighbors for edge iter
                GCounts counts = new GCounts(0,0,0,0,0,0,0,0);
//                System.out.println(u + "\t" + v);    // for checking
                int[] v_neighbors = H.successorArray(v);
                int v_deg = H.outdegree(v);
                int uvIdx = findIdx_BS(v_neighbors, 0, v_deg, u);  
                if (uvIdx <= v_deg-1) {    // < // has neighbor(s) bigger than u

                    long g1 = 0;   // wedge 
                    long g2 = 0;   // triangle
                    long g3 = 0;   // 4-node path
                    long g4 = 0;   // 3-star
                    long g5 = 0;   // 4-cycle
                    long g6 = 0;   // lollipop
                    long g7 = 0;   // diamond
                    long g8 = 0;   // 4-clique

                    for(int i=0,j=uvIdx+1; i<u_Bgdeg || j<v_deg; ) {
                        int uP, vP;
                        if (i<u_Bgdeg){
                            uP = u_Bgneighbors[i];
                        } else {
                            uP = n+1;
                        }
                        if(j<v_deg) {
                            vP = v_neighbors[j];
                        } else {
                            vP = n+2;
                        }

                        if(uP == vP) {  // Find a triangle !
                            int w =  uP;
                            if(w>v) {  // to avoid double counting triangle
                                g2++;
//                                System.out.println("Triangle: " + u + ", " + v + ", " + w);
               // now find g6,g7,g8:
                                int[] w_neighbors = H.successorArray(w);
                                int w_deg = H.outdegree(w);
                                int uwIdx = findIdx_BS(w_neighbors, 0, w_deg, u);  
//                                if (uwIdx < w_deg-1) {   // has neighbor(s) bigger than u
//  This is if we neglect lollipops.
//                                ThreeCounts int3 = explore_Triangle(u, v, w,  u_Bgneighbors, v_neighbors, w_neighbors, u_Bgdeg, uvIdx+1, v_deg, uwIdx+1, w_deg);
                                ThreeCounts int3 = explore_Triangle(u, v, w,  u_neighbors, v_neighbors, w_neighbors, u_deg, 0, v_deg, 0, w_deg);
                                g6 += int3.n1;
                                g7 += int3.n2;
                                g8 += int3.n3;
//                                }
                            }
                            i++; j++;
                            continue;
                        }

                        if((uP < vP) || (vP == u)) {     //(uP < vP) { // find wedge v-u-w (type-1)
                            int w = uP;
                            if(w>v) {   // to avoid double counting wedge
                                g1++;
//                                System.out.println("Wedge-1: " + v + ", " + u + ", " + w);
                // now find g3,g4,g5:
                                int[] w_neighbors = H.successorArray(w);
                                int w_deg = H.outdegree(w);
                                int uwIdx = findIdx_BS(w_neighbors, 0, w_deg, u);  
                                ThreeCounts int3 = explore_Wedge1(u, v, w,  u_Bgneighbors, v_neighbors, w_neighbors, u_Bgdeg, uvIdx+1, v_deg, uwIdx+1, w_deg);
                                g3 += int3.n1;
                                g4 += int3.n2;
                                g5 += int3.n3;
                            }
                            i++;
                            continue;
                        }

                        if((uP > vP) && (vP != u)){    //  (uP > vP) { // find wedge u-v-w (type-2)
                            int w = vP;
                   // In this case, v can be < or > w
                   // But both v>u and w>u to avoid double counting -- use set >u
                            g1++;
//                            System.out.println("Wedge-2: " + u + ", " + v + ", " + w);
               // now find g3,g4,g5:
                            int[] w_neighbors = H.successorArray(w);
                            int w_deg = H.outdegree(w);
                            int uwIdx = findIdx_BS(w_neighbors, 0, w_deg, u);  
                            ThreeCounts int3 = explore_Wedge2(u, v, w,  u_Bgneighbors, v_neighbors, w_neighbors, u_Bgdeg, uvIdx+1, v_deg, uwIdx+1, w_deg);
                            g3 += int3.n1;
                            g4 += int3.n2;
                            g5 += int3.n3;
                            j++;
                            continue;
                        }
                    }

                    counts.ng1 = g1;
                    counts.ng2 = g2;
                    counts.ng3 = g3;
                    counts.ng4 = g4;
                    counts.ng5 = g5;
                    counts.ng6 = g6;
                    counts.ng7 = g7;
                    counts.ng8 = g8;
                }

//             } 

             return counts; 

		  }).<GCounts>reduce(new GCounts(0,0,0,0,0,0,0,0),GCounts::add);
          return uv_counts;
      }).<GCounts>reduce(new GCounts(0,0,0,0,0,0,0,0),GCounts::add);


      System.out.println("Results:");
      System.out.println("Wedges    (g1): " + tot_counts.ng1);
      System.out.println("Triangles (g2): " + tot_counts.ng2);
      System.out.println("4-Paths   (g3): " + tot_counts.ng3);
      System.out.println("3-stars   (g4): " + tot_counts.ng4);
      System.out.println("4-cycles  (g5): " + tot_counts.ng5);
      System.out.println("Lollipops (g6): " + tot_counts.ng6);
      System.out.println("Diamonds  (g7): " + tot_counts.ng7);
      System.out.println("4-cliques (g8): " + tot_counts.ng8);
   }
	
	
   ThreeCounts explore_Triangle(int u, int v, int w, int[] u_neighbors, int[] v_neighbors, int[] w_neighbors, int u_deg, int v_0, int v_deg, int w_0, int w_deg) {

// Here u < v < w.

      int g6 = 0;
      int g7 = 0;
      int g8 = 0;		

      for(int i=0,j=v_0,k=w_0; i<u_deg || j<v_deg || k<w_deg; ) {
         int uP, vP, wP;

         if(i<u_deg){
             uP = u_neighbors[i];
         } else {
             uP = n+1;
         }
         if(j<v_deg) {
             vP = v_neighbors[j];
         } else {
             vP = n+2;
         }
         if(k<w_deg){
             wP = w_neighbors[k];
         } else {
             wP = n+3;
         }

	
         if(uP == vP && uP == wP) { // Find a 4-clique !
            int z = uP;
            if (z > w) { // to avoid multiple counting. Here assuming u<v<w.
               g8++;
//               System.out.println("4-clique: " + u + ", " + v + ", " + w  + ", " + z); // for checking
            }
            i++; j++; k++;
            continue;
         }
			
         if(uP == vP && uP < wP) { // a diamond of type 2
            int z = uP;
            if (z > w) {   // to avoid multiple counting
               g7++;
//               System.out.println("diamond: " + u + "--" + v + ", " + w  + ", " + z); // for checking
            }
            i++; j++;
            continue;
         }

         if(uP == wP && uP < vP) { // a diamond of type 2
            int z = uP;
            if (z > v) {   // to avoid multiple counting
               g7++;
//               System.out.println("diamond: " + u + "--" + w + ", " + v  + ", " + z); // for checking
            }
            i++; k++; 
            continue;
         }

         if(uP < wP && uP < vP){ // a lollipop at u
            int z = uP;
            g6++;
//            System.out.println("lollipop: ([" + u + "], " + v + ", " + w  + "), " + z); // for checking
            i++; 
            continue;
         } 

         if(vP < uP && vP == wP){  // a diamond of type 1
            int z = vP;
            if (z > u) { // to avoid multiple counting
               g7++;
//               System.out.println("diamond: " + u + ", " + v + "--" + w  + ", " + z); // for checking
            }
            k++; j++;
            continue;
         }

         if(vP < uP && vP < wP){  // a lollipop at v
            int z = vP;
            g6++;
//            System.out.println("lollipop: (" + u + ", [" + v + "], " + w  + "), " + z); // for checking
            j++;
            continue;
         }

         if(wP < uP && wP < vP){  // a lollipop at w
            int z = wP;
            g6++;
//            System.out.println("lollipop: (" + u + ", " + v + ", [" + w  + "]), " + z); // for checking
            k++; 
            continue;
         } 

      }
      return new ThreeCounts(g6, g7, g8);
   }


   ThreeCounts explore_Wedge1(int u, int v, int w, int[] u_neighbors, int[] v_neighbors, int[] w_neighbors, int u_deg, int v_0, int v_deg, int w_0, int w_deg) {

// Here u < v < w.
// The wedge is v-u-w

      int g3 = 0;
      int g4 = 0;
      int g5 = 0;		

      for(int i=0,j=v_0,k=w_0; i<u_deg || j<v_deg || k<w_deg; ) {
         int uP, vP, wP;

         if(i<u_deg){
             uP = u_neighbors[i];
         } else {
             uP = n+1;
         }
         if(j<v_deg) {
             vP = v_neighbors[j];
         } else {
             vP = n+2;
         }
         if(k<w_deg){
             wP = w_neighbors[k];
         } else {
             wP = n+3;
         }
	
         if(uP == vP && uP == wP) { // a diamond -- should be found through triangle! - so do nothing
            i++; j++; k++;
            continue;
         }
			
         if(uP == vP && uP < wP) { // a lollipop -- should be found through triangle! - so do nothing
            i++; j++;
            continue;
         }

         if(uP == wP && uP < vP) { // a lollipop -- should be found through triangle! - so do nothing
            i++; k++; 
            continue;
         }

         if(uP < wP && uP < vP){ // a star centered at u
            int z = uP;
            if (z > w) { // to avoid double counting
               g4++;
//               System.out.println("star: [" + u + "], " + v + ", " + w  + ", " + z); // for checking
            }
            i++; 
            continue;
         } 

         if(vP < uP && vP == wP){  // a rectangle
            int z = vP;
            if (z > u) { // to avoid multiple counting
               g5++;
//               System.out.println("rectangle: " + u + ", " + v + ", " + z  + ", " + w); // for checking
            }
            k++; j++;
            continue;
         }

         if(vP < uP && vP < wP){  // a path
            int z = vP;
            if (z > u){    // to avoid multiple counting
               g3++;
//               System.out.println("path: " + w + ", " + u + ", " + v  + ", " + z); // for checking
            }
            j++;
            continue;
         }

         if(wP < uP && wP < vP){  // a path
            int z = wP;
            if (z > u){    // to avoid multiple counting
               g3++;
//               System.out.println("path: " + v + ", " + u + ", " + w  + ", " + z); // for checking
            }
            k++; 
            continue;
         } 
      }
      return new ThreeCounts(g3, g4, g5);
   }

   ThreeCounts explore_Wedge2(int u, int v, int w, int[] u_neighbors, int[] v_neighbors, int[] w_neighbors, int u_deg, int v_0, int v_deg, int w_0, int w_deg) {

// Here u < v, and u < w, but v and w are unordered.
// The wedge is u-v-w.

      int g3 = 0;
      int g4 = 0;
      int g5 = 0;		

      for(int i=0,j=v_0,k=w_0; i<u_deg || j<v_deg || k<w_deg; ) {
         int uP, vP, wP;

         if(i<u_deg){
             uP = u_neighbors[i];
         } else {
             uP = n+1;
         }
         if(j<v_deg) {
             vP = v_neighbors[j];
         } else {
             vP = n+2;
         }
         if(k<w_deg){
             wP = w_neighbors[k];
         } else {
             wP = n+3;
         }
	
         if(uP == vP && uP == wP) { // a diamond -- should be found through triangle! - so do nothing
            i++; j++; k++;
            continue;
         }
			
         if(uP == vP && uP < wP) { // a lollipop -- should be found through triangle! - so do nothing
            i++; j++;
            continue;
         }

         if(uP == wP && uP < vP) { // a rectangle -- should be found through wedge1! - so do nothing
            i++; k++; 
            continue;
         }

         if(uP < wP && uP < vP){ // a path -- which should be found through wedge1! - so do nothing
            i++; 
            continue;
         } 

         if(vP < uP && vP == wP){  // a lollipop -- should be found through triangle! - so do nothing
            k++; j++;
            continue;
         }

         if(vP < uP && vP < wP){  // a star centered at v
            int z = vP;
            if (z > u  && z > w){    // to avoid multiple counting
               g4++;
//               System.out.println("star: " + u + ", [" + v + "], " + w  + ", " + z); // for checking
            }
            j++;
            continue;
         }

         if(wP < uP && wP < vP){  // a path
            int z = wP;
            if (z > u && z != v){    // to avoid multiple counting and loop
               g3++;
//               System.out.println("path: " + u + ", " + v + ", " + w + ", " + z); // for checking
            }
            k++; 
            continue;
         } 
      }
      return new ThreeCounts(g3, g4, g5);
   }

    int findIdx_BS(int[] arr, int aLow, int aHigh, int val) {
        // using binary search, assuming array is sorted
        // aLow is the first index in the range being searched
        // aHigh is the upper boundary (exclusive)
        if (aLow == aHigh-1) {
            if (arr[aLow] == val){
               return aLow;
            } else {
               return -1;     // not found
            }
        }
        int midIdx = (aHigh-aLow)/2 + aLow;   // floor for integer division
        if (arr[midIdx] > val){
            return findIdx_BS(arr, aLow, midIdx, val);
        } else if (arr[midIdx] < val){
            return findIdx_BS(arr, midIdx, aHigh, val);
        } else {   // equal
            return midIdx;
        }
    }


   public static void main(String[] args) throws Exception {
      long startTime = System.currentTimeMillis();
		
      String basename = args[0]; 
		
      FourGraphlets t = new FourGraphlets(basename);

      t.compute();
		
      System.out.println("Total time elapsed = " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

   }
}

class GCounts{
    long ng1;
    long ng2;
    long ng3;
    long ng4;
    long ng5;
    long ng6;
    long ng7;
    long ng8;
    public GCounts(long ng1, long ng2, long ng3, long ng4, long ng5, long ng6, long ng7, long ng8){
        this.ng1 = ng1;
        this.ng2 = ng2;
        this.ng3 = ng3;
        this.ng4 = ng4;
        this.ng5 = ng5;
        this.ng6 = ng6;
        this.ng7 = ng7;
        this.ng8 = ng8;
    }
    public GCounts add(GCounts A){
        long ng1new = this.ng1 + A.ng1;
        long ng2new = this.ng2 + A.ng2;
        long ng3new = this.ng3 + A.ng3;
        long ng4new = this.ng4 + A.ng4;
        long ng5new = this.ng5 + A.ng5;
        long ng6new = this.ng6 + A.ng6;
        long ng7new = this.ng7 + A.ng7;
        long ng8new = this.ng8 + A.ng8;
        return new GCounts(ng1new, ng2new, ng3new, ng4new, ng5new, ng6new, ng7new, ng8new);
    }
}

class ThreeCounts{
    int n1;
    int n2;
    int n3;

    public ThreeCounts(int n1, int n2, int n3){
        this.n1 = n1;
        this.n2 = n2;
        this.n3 = n3;
    }
}


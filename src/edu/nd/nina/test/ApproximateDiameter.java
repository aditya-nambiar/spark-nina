package edu.nd.nina.test;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.GraphLoader;

public class ApproximateDiameter {

	public static void main(String[] args){
		
		if (args.length < 2) {
			System.err.println("Usage: ApproximateDiameter <master> <file>");
			System.exit(1);
		}
		
		System.out.println("Approximate graph diameter");		
		
		SparkContext ctx = new SparkContext(args[0], "ApproximateDiameter",
			      System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(ApproximateDiameter.class));

		GraphLoader.edgeListFile(ctx, args[1]);
		
		JavaRDD<String> lines = ctx.textFile(args[1], 1);
		
		float termination_criteria = 0.0001f;
		boolean use_sketch = false;

		lines.map
		
		
//
//			  //load graph
//			  graph_type graph(dc, clopts);
//			  dc.cout() << "Loading graph in format: "<< format << std::endl;
//			  graph.load_format(graph_dir, format);
//			  graph.finalize();
//
//			  time_t start, end;
//			  //initialize vertices
//			  time(&start);
//			  if (use_sketch == false)
//			    graph.transform_vertices(initialize_vertex);
//			  else
//			    graph.transform_vertices(initialize_vertex_with_hash);
//
//			  graphlab::omni_engine<one_hop> engine(dc, graph, exec_type, clopts);
//
//			  //main iteration
//			  size_t previous_count = 0;
//			  size_t diameter = 0;
//			  for (size_t iter = 0; iter < 100; ++iter) {
//			    engine.signal_all();
//			    engine.start();
//
//			    graph.transform_vertices(copy_bitmasks);
//
//			    size_t current_count = 0;
//			    if (use_sketch == false)
//			      current_count = graph.map_reduce_vertices<size_t>(absolute_vertex_data);
//			    else
//			      current_count = graph.map_reduce_vertices<size_t>(
//			          absolute_vertex_data_with_hash);
//			    dc.cout() << iter + 1 << "-th hop: " << current_count
//			        << " vertex pairs are reached\n";
//			    if (iter > 0
//			        && (float) current_count
//			            < (float) previous_count * (1.0 + termination_criteria)) {
//			      diameter = iter;
//			      dc.cout() << "converge\n";
//			      break;
//			    }
//			    previous_count = current_count;
//			  }
//			  time(&end);
//
//			  dc.cout() << "graph calculation time is " << (end - start) << " sec\n";
//			  dc.cout() << "The approximate diameter is " << diameter << "\n";
//
//			  graphlab::mpi_tools::finalize();
//
//			  return EXIT_SUCCESS;
//			}
		}
	
}

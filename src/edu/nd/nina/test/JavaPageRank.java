package edu.nd.nina.test;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Iterables;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
public final class JavaPageRank {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum extends Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JavaPageRank <master> <file> <number_of_iterations>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(args[0], "JavaPageRank",
      System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaPageRank.class));

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, List<String>> links = lines.map(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<List<String>, Double>() {
      @Override
      public Double call(List<String> rs) {
        return 1.0;
      }
    });

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < Integer.parseInt(args[2]); current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMap(new PairFlatMapFunction<Tuple2<List<String>, Double>, String, Double>() {
          @Override
          public Iterable<Tuple2<String, Double>> call(Tuple2<List<String>, Double> s) {
	    int urlCount = Iterables.size(s._1);
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1) {
              results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
            }
            return results;
          }
      });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }

    // Collects all URL ranks and dump them to console.
    List<Tuple2<String, Double>> output = ranks.collect();
    for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
    }

    ctx.stop();
  }
}


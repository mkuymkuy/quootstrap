package ch.epfl.dlab.quootstrap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class Exporter {

	private final JavaPairRDD<String, String> quotationMap;
	private final JavaPairRDD<List<Token>, Tuple2<String, String>> freebaseMapping;
	
	/** Map article UID to a tuple (full quotation, website, date) */
	private final JavaPairRDD<Tuple2<Long, Integer>, Tuple3<String, String, String>> articles;
	
	public Exporter(JavaSparkContext sc, JavaRDD<Sentence> sentences, NameDatabase people)  {
		
		final boolean caseSensitive = ConfigManager.getInstance().isCaseSensitive();
		
		// Map speakers to their unique Freebase ID
		this.freebaseMapping = people.getPeopleRDD()
				.mapToPair(x -> new Tuple2<>(
						caseSensitive ? Token.getTokens(x._1) : Token.caseFold(Token.getTokens(x._1)),
						new Tuple2<>(x._2, String.join(" ", x._1))
					))
				.reduceByKey((x, y) -> x._2.compareTo(y._2) == -1 ? x : y);
		
		final Set<String> langSet = new HashSet<>(ConfigManager.getInstance().getLangFilter());
		this.articles = QuotationExtraction.getConcreteDatasetLoader().loadArticles(sc,
				ConfigManager.getInstance().getDatasetPath(), langSet)
			.mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getWebsite(), x.getDate()), new Tuple2<>(x.getArticleContent(), x.getArticleUID())))
			.flatMapValues(x -> ContextExtractor.extractQuotations(x._1(), x._2()))
			.mapToPair(x -> new Tuple2<>(x._2.getKey(), new Tuple3<>(x._2.getQuotation(), x._1._1, x._1._2)));
		
		this.quotationMap = computeQuotationMap(sc);
	}
	
	private JavaPairRDD<String, String> computeQuotationMap(JavaSparkContext sc) {
		Set<String> langSet = new HashSet<>(ConfigManager.getInstance().getLangFilter());
		
		// Reconstruct quotations (from the lower-case canonical form to the full form)
		return QuotationExtraction.getConcreteDatasetLoader().loadArticles(sc,
				ConfigManager.getInstance().getDatasetPath(), langSet)
			.flatMap(x -> ContextExtractor.extractQuotations(x.getArticleContent(), x.getArticleUID()).iterator())
			.mapToPair(x -> new Tuple2<>(StaticRules.canonicalizeQuotation(x.getQuotation()), x.getQuotation()))
			.reduceByKey((x, y) -> {
				// Out of multiple possibilities, get the longest quotation
				if (x.length() > y.length()) {
					return x;
				} else if (x.length() < y.length()) {
					return y;
				} else {
					// Lexicographical comparison to ensure determinism
					return x.compareTo(y) == -1 ? x : y;
				}
			});
	}
	
	public void exportResults(JavaPairRDD<String, Tuple2<List<Token>, LineageInfo>> pairs) {
		String exportPath = ConfigManager.getInstance().getExportPath();
		final boolean caseSensitive = ConfigManager.getInstance().isCaseSensitive();
		
		JavaPairRDD<String, Tuple4<Tuple2<Long, Integer>, String, String, String>> articleMap = pairs.mapToPair(x -> new Tuple2<>(x._1, x._2._2)) // (canonical quotation, lineage info)
			.flatMapValues(x -> {
				// (key)
				List<Tuple2<Long, Integer>> values = new ArrayList<>();
				for (int i = 0; i < x.getPatterns().size(); i++) {
					values.add(x.getSentences().get(i).getKey());
				}
				return values;
			}) // (canonical quotation, key)
			.mapToPair(Tuple2::swap) // (key, canonical quotation)
			.join(this.articles) // (key, (canonical quotation, (website, date)))
			.mapToPair(x -> new Tuple2<>(x._2._1, new Tuple4<>(x._1, x._2._2._1(), x._2._2._2(), x._2._2._3()))); // (canonical quotation, (key, full quotation, website, date))
		
		pairs // (canonical quotation, (speaker, lineage info))
			.join(quotationMap)
			.mapValues(x -> new Tuple3<>(x._1._1.toString(), x._1._2, x._2)) // (canonical quotation, (speaker,lineage info, full quotation))
			//.mapToPair(x -> new Tuple2<>(caseSensitive ? x._2._1() : Token.caseFold(x._2._1()), new Tuple3<>(x._1, x._2._2(), x._2._3()))) // (speaker, (canonical quotation, lineage info, full quotation))
			//.leftOuterJoin(freebaseMapping) // (speaker, ((canonical quotation, lineage info, full quotation), Freebase ID, Freebase Speaker))
			//.mapToPair(x -> new Tuple2<>(x._2._1._1(), new Tuple4<>(x._1, x._2._2._1, x._2._1._2(), x._2._1._3()))) // (canonical quotation, (speaker, Freebase ID of the speaker, lineage info, full quotation))
			.cogroup(articleMap)//(canonical quotation, (speaker, lineage info, full quotation)) join (canonical quotation, (key, full quotation, website, date))
			.map(t -> {
				
				String canonicalQuotation = t._1;
				Map<Tuple2<Long, Integer>, Tuple3<String, String, String>> articles = new HashMap<>();
				t._2._2.forEach(x -> {
					articles.put(x._1(), new Tuple3<>(x._2(), x._3(), x._4())); // (key, (full quotation, website, date))
				});
				
				Tuple3<String, LineageInfo, String> data = t._2._1.iterator().next();
				
				JsonObject o = new JsonObject();
				o.addProperty("quotation", data._3());
				o.addProperty("canonicalQuotation", canonicalQuotation);
				o.addProperty("speaker", data._1());
				//o.addProperty("speakerID", data._2() != null ? data._2().substring(1, data._2().length() - 1) : ""); // Remove < and > from the Freebase ID
				o.addProperty("confidence", data._2().getConfidence()); // Tuple confidence
				
				JsonArray occurrences = new JsonArray();
				for (int i = 0; i < data._2().getPatterns().size(); i++) {
					JsonObject occ = new JsonObject();
					Tuple2<Long, Integer> key = data._2().getSentences().get(i).getKey();
					occ.addProperty("articleUID", Long.toString(key._1));
					occ.addProperty("articleOffset", data._2().getSentences().get(i).getIndex());
					occ.addProperty("extractedBy", data._2().getPatterns().get(i).toString(false));
					occ.addProperty("patternConfidence", data._2().getPatterns().get(i).getConfidenceMetric());
					occ.addProperty("quotation", articles.get(key)._1());
					occ.addProperty("website", articles.get(key)._2());
					String date = articles.get(key)._3();
					if (!date.isEmpty()) {
						occ.addProperty("date", date);
					}
					occurrences.add(occ);
				}
				o.add("occurrences", occurrences);

				return new GsonBuilder().disableHtmlEscaping().create().toJson(o);
			})
			.saveAsTextFile(exportPath);
	}
	
}

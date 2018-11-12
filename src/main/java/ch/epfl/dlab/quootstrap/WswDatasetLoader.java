package ch.epfl.dlab.quootstrap;

import java.util.Set;
import java.util.List;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple4;

public class WswDatasetLoader implements DatasetLoader {

	private static SparkSession session;
	
	private static void initialize(JavaSparkContext sc) {
		if (session == null) {
			session = new SparkSession(sc.sc());
		}
	}
	
	@Override
	public JavaRDD<DatasetLoader.Article> loadArticles(JavaSparkContext sc, String datasetPath, Set<String> languageFilter) {
		WswDatasetLoader.initialize(sc);
		return sc.textFile(datasetPath).map(x -> {
				String[] data = x.split("\t");
				final Long ID = Long.parseLong(data[0]);
				final String url = data[1];
			
			    List<String> tokenizedContent = Arrays.asList(data[2].split(" "));
			    String time = data[3].replaceAll("\n","");
			    return new Article(Long.parseLong(data[0]), tokenizedContent, url, time);
			});
	}
	public static class Article implements DatasetLoader.Article {
		
		private final long articleUID;
		private final List<String> articleContent;
		private final String website;
		private final String date;
		
		public Article(long articleUID, List<String> articleContent, String website, String date) {
			this.articleUID = articleUID;
			this.articleContent = articleContent;
			this.website = website;
			this.date = date;
		}
		
		@Override
		public long getArticleUID() {
			return articleUID;
		}

		@Override
		public List<String> getArticleContent() {
			return articleContent;
		}

		@Override
		public String getWebsite() {
			return website;
		}

		@Override
		public String getDate() {
			return date;
		}
		
	}
}
		

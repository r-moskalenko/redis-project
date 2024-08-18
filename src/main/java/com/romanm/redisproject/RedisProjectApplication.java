package com.romanm.redisproject;

import com.github.javafaker.Faker;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.Suggestion;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

@SpringBootApplication
public class RedisProjectApplication {

	Logger logger = LoggerFactory.getLogger(RedisProjectApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(RedisProjectApplication.class, args);
	}

	@Bean
	CommandLineRunner createSearchIndices(JedisConnectionFactory cf) {
		return args -> {
			var searchIndexName = "article-idx";

			try (var client = new Client(searchIndexName, cf.getHostName(), cf.getPort())) { //TODO Not sure of import
				var sc = new Schema()
						.addSortableTextField("title", 1.0)
						.addSortableNumericField("price");

				var indexDefinition = new IndexDefinition().setPrefixes(String.format("%s:", Article.class.getName()));

				client.dropIndex();
				client.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));
			} catch (JedisDataException e) {
				e.printStackTrace();
			}
		};
	}

	@Bean
	JedisConnectionFactory jedisConnectionFactory() {
		return new JedisConnectionFactory();
	}

	@Bean
	RedisTemplate<String, String> redisTemplate() {
		RedisTemplate<String, String> template = new RedisTemplate<>();
		template.setConnectionFactory(jedisConnectionFactory());
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new StringRedisSerializer());

		return template;
	}

	static Map<ArticleType, String> titles = Map.of(
			ArticleType.INFLUENCE, "The influence of %s on %s",
			ArticleType.LEARN, "What I learn about %s from %s",
			ArticleType.COOK, "How to cook %s with %s",
			ArticleType.TEACH, "Teach your %s how to pilot a %s"
	);

	private static String getFakeArticleTitle(Faker faker) {
		var keys = titles.keySet();
		var rando = (long) (keys.size() * Math.random());
		var key = keys.stream().skip(rando).findAny().get();
		var param1 = "";
		var param2 = "";

		switch (key) {
			case INFLUENCE:
				param1 = faker.ancient().god();
				param2 = faker.food().dish();
				break;
			case LEARN:
				param1 = faker.artist().name();
				param2 = faker.backToTheFuture().character();
				break;
			case COOK:
				param1 = faker.food().dish();
				param2 = StringUtils.capitalize(faker.animal().name());
				break;
			case TEACH:
				param1 = faker.animal().name();
				param2 = faker.aviation().aircraft();
				break;
		}

		return String.format(titles.get(key), param1, param2);
	}

	@Bean
	CommandLineRunner makeMeSomeData(ArticleRepository articleRepository, AuthorRepository authorRepository, RedisTemplate<String, String> redisTemplate, JedisConnectionFactory cf) {
		return args -> {
			if (articleRepository.count() == 0) {
				var faker = new Faker();

				IntStream.range(0, 250).forEach(n -> {
					var author = new Author(null, faker.name().fullName());
					authorRepository.save(author);
				});

				var random = new Random();
				var titles = new HashSet<String>();

				IntStream.range(0, 2500).forEach(n -> {
					var articleTitle = getFakeArticleTitle(faker);

					while (titles.contains(articleTitle)) {
						articleTitle = getFakeArticleTitle(faker);
					}

					titles.add(articleTitle);

					var article = new Article(null, articleTitle, Double.parseDouble(faker.commerce().price()), new HashSet<>());
					IntStream.range(0, random.nextInt(2) + 1).forEach(j -> {
						var authorId = redisTemplate.opsForSet().randomMember(Author.class.getName());
						article.addAuthor(new Author(authorId, null));
					});
					articleRepository.save(article);
				});
			}
		};
	}
}

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/articles")
class ArticlesController {

	private final JedisConnectionFactory cf;

	private final ArticleRepository articleRepository;

	@GetMapping("/search")
	SearchResult search(@RequestParam(name = "q") String query,
						@RequestParam(defaultValue = "-1") Double minPrice,
						@RequestParam(defaultValue = "-1") Double maxPrice) {
		try(var client = new Client("article-idx", cf.getHostName(), cf.getPort())) {
			var q = new Query(query);

			if (minPrice != -1 && maxPrice != -1) {
				q.addFilter(new Query.NumericFilter("price", minPrice, maxPrice));
			}
			q.returnFields("title", "price");

			return client.search(q);
		}
	}

	@GetMapping()
	Iterable<Article> getAllArticles() {
		return articleRepository.findAll();
	}

	@GetMapping("/authors")
	Collection<Suggestion> authorAutoComplete(@RequestParam(name = "q") String query) {
		return null;
	}
}

@Data
@AllArgsConstructor
@RedisHash
class Author {
	@Id
	private String id;
	private String name;
}

@Data
@AllArgsConstructor
@RedisHash
class Article {
	@Id
	private String id;
	private String title;
	private Double price;

	@Reference
	private Set<Author> authorSet;

	public void addAuthor(Author author) {
		authorSet.add(author);
	}
}

interface ArticleRepository extends CrudRepository<Article, String> {}

interface AuthorRepository extends CrudRepository<Author, String> {}

enum ArticleType {
	INFLUENCE, LEARN, COOK, TEACH
}
package com.romanm.redisproject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.Flux;

import java.util.Map;

@SpringBootApplication
public class RedisProjectApplication {

	Logger logger = LoggerFactory.getLogger(RedisProjectApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(RedisProjectApplication.class, args);
	}

	@Bean
	public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
		return new LettuceConnectionFactory("127.0.0.1", 6379);
	}

	@Bean
	ApplicationRunner geography(ReactiveRedisTemplate<String, String> template) {
		return args -> {
			var geoTemplate = template.opsForGeo();

			var sicily = "Sicily";
			var mapOfPoints = Map.of(
					"Arigento", new Point(13.361389, 38.1155556),
					"Catania", new Point(15.0076269, 37.502669),
					"Palermo", new Point(13.5833333, 37.316667)
			);
			Flux.fromIterable(mapOfPoints.entrySet())
					.flatMap(e -> geoTemplate.add(sicily, e.getValue(), e.getKey()))
					.thenMany(geoTemplate.radius(sicily, new Circle(
							new Point(13.583333, 37.31667),
							new Distance(10, RedisGeoCommands.DistanceUnit.KILOMETERS))))
					.map(GeoResult::getContent)
					.map(RedisGeoCommands.GeoLocation::getName)
					.doOnNext(System.out::println)
					.subscribe();
		};
	}

	@Bean
	ApplicationRunner list(ReactiveRedisTemplate<String, String> template) {
		return args -> {
			var listTemplate = template.opsForList();
			var listName = "spring-team";
			var push = listTemplate.leftPushAll(listName, "Madhura", "Josh", "Dr. Syer", "Olga", "Violetta");

			push.thenMany(listTemplate.leftPop(listName))
					.doOnNext(System.out::println)
					.thenMany(listTemplate.leftPop(listName))
					.doOnNext(System.out::println)
					.subscribe();
		};
	}
}

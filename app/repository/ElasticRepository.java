package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, final int size, final int page) {
        String json = "{\n" +
                "  \"from\": " + size * (page - 1) + ",\n" +
                "  \"size\": " + size + ", \n" +
                "  \"query\": {\n" +
                "    \"query_string\" : {\n" +
                "      \"query\": \"*" + input + "*\",\n" +
                "      \"fields\": [\n" +
                "        \"name.keyword^4\",\n" +
                "        \"aliases.keyword^3\",\n" +
                "        \"secretIdentities.keyword^3\", \n" +
                "        \"description.keyword^2\",\n" +
                "        \"partners.keyword^1\"\n" +
                "        ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(json))
                .thenApply(response -> {
                    JsonNode hits = response.asJson().get("hits");
                    int total = hits.get("total").get("value").asInt();
                    int totalPage = 1 + total / size;
                    List<SearchedHero> heroes = StreamSupport.stream(hits.get("hits").spliterator(), false)
                            .map(jsonNode -> SearchedHero.fromJson(jsonNode.get("_source"))).collect(Collectors.toList());
                    return new PaginatedResults<>(total, page, totalPage, heroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        String json = "{\n" +
                "  \"suggest\": {\n" +
                "    \"suggestion\": {\n" +
                "      \"prefix\": \"" + input + "\",\n" +
                "      \"completion\": {\n" +
                "        \"field\": \"suggest\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(json))
                .thenApply(response -> {
                    Spliterator<JsonNode> suggestions = response.asJson().get("suggest").get("suggestion").spliterator();
                    return StreamSupport.stream(suggestions, false)
                            .flatMap(suggestion -> StreamSupport.stream(suggestion.get("options").spliterator(), false)
                                    .map(option -> SearchedHero.fromJson(option.get("_source")))
                            ).collect(Collectors.toList());
                });
    }
}

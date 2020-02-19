package repository;

import io.lettuce.core.RedisClient;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");

    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> aBoolean && aLong > 0);
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        return redisClient.connect().async().zincrby("inTops", 1, statItem.toJson().toString()).thenApply(a -> true);
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        return redisClient.connect().async().zadd("lastVisited", new Date().getTime(), statItem.toJson().toString());
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        return redisClient.connect().async().zrevrange("lastVisited", 0, count-1)
                .thenApply(lastHeroesVisited -> lastHeroesVisited.stream().map(StatItem::fromJson).collect(Collectors.toList()));
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        return redisClient.connect().async().zrevrangeWithScores("inTops", 0, count-1)
                .thenApply(scoredValues -> scoredValues.stream().map(scoredValue ->
                        new TopStatItem(StatItem.fromJson(scoredValue.getValue()), ((long) scoredValue.getScore()))).collect(Collectors.toList()));
    }
}

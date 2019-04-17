package com.nataliaar.sethefinalproject.eventgenerator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.net.util.SubnetUtils;

public class RandomEventProducer {
    private static final String GEO_LITE_2_DB_URL = "GeoLite2-Country-CSV_20190409/GeoLite2-Country-Blocks-IPv4.csv";

    private Random random;
    private GeoLite2Data geoLite2Data;

    public RandomEventProducer() throws IOException, URISyntaxException {
        this.random = new Random();
        URI geo2Uri = ClassLoader.getSystemResource(GEO_LITE_2_DB_URL).toURI();
        Map<String, String> env = new HashMap<String, String>();
        FileSystems.newFileSystem(geo2Uri, env);
        this.geoLite2Data = new GeoLite2Data(geo2Uri);
    }

    public ProductPurchaseEvent buildRandomEvent() {
        ProductPurchaseEvent event = new ProductPurchaseEvent();

        event.setProductName(generateRandomName("Product_", 100));
        event.setProductPrice(generatePrice());
        event.setPurchaseDate(generatePurchaseDate());
        event.setProductCategory(generateRandomName("Category_", 20));
        event.setClientIpAddress(generateRandomIp());

        return event;
    }

    protected String generateRandomName(String name, int limit) {
        return name + getRandom().nextInt(limit);
    }

    protected Double generatePrice() {
        double price = 0;
        while (price < 99 || price > 9999.99) {
            price = 5000 + getRandom().nextGaussian() * 2500;
            price = Math.floor(price * 100) / 100;
        }
        return price;
    }

    protected LocalDateTime generatePurchaseDate() {
        LocalDate start = LocalDate.now().minusDays(7);
        LocalDate end = LocalDate.now();
        long date = getRandom().longs(1, start.toEpochDay(), end.toEpochDay()).findAny().getAsLong();
        int hour = getGaussianInt(12, 6, 0, 23);
        int minute = getGaussianInt(30, 20, 0, 59);
        return LocalDateTime.of(LocalDate.ofEpochDay(date), LocalTime.of(hour, minute));
    }

    protected String generateRandomIp() {
        String[] allAddresses;
        do {
            int randomIndex = random.nextInt(geoLite2Data.getNetworkDataSize());
            String networkData = geoLite2Data.getNetworkData(randomIndex);
            allAddresses = new SubnetUtils(networkData).getInfo().getAllAddresses();
        } while (allAddresses.length <= 0);
        
        return allAddresses[random.nextInt(allAddresses.length)];
    }

    protected Random getRandom() {
        return random;
    }

    private int getGaussianInt(int mean, int dev, int lower, int upper) {
        int num = (int) Math.floor(mean + getRandom().nextGaussian() * dev);
        while (num < lower || num > upper) {
            num = (int) Math.floor(mean + getRandom().nextGaussian() * dev);
        }
        return num;
    }
}

package com.kafka.learning.producer.model;

import com.google.gson.Gson;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class StockPrice {
    private final int dollars;
    private final int cents;
    private final String name;

    public StockPrice(final String json) {
        this(new Gson().fromJson(json, StockPrice.class));
    }

    public StockPrice() {
        dollars = 0;
        cents = 0;
        name = "";
    }

    public StockPrice(final String name, final int dollars, final int cents) {
        this.dollars = dollars;
        this.cents = cents;
        this.name = name;
    }

    public StockPrice(final StockPrice stockPrice) {
        this.cents = stockPrice.cents;
        this.dollars = stockPrice.dollars;
        this.name = stockPrice.name;
    }

    public String toJson() {
        return "{" + "\"dollars\": " + dollars + ", \"cents\": " + cents + ", \"name\": \"" + name + '\"' + '}';
    }
}

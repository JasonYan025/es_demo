package com.yandong025.es.util;

import com.yandong025.es.pojo.Product;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {

    public static void main(String[] args) throws IOException {
        List<Product> list = getProductByKeyWord("爆发");
        System.out.println(list);
    }


    public static List<Product> getProductByKeyWord(String keyword) throws IOException {
        keyword = URLEncoder.encode(keyword, "utf-8");
        String url = "https://search.jd.com/Search?keyword=" + keyword + "&enc=utf-8";
        Document document = Jsoup.parse(new URL(url), 30000);
        Element element = document.getElementById("J_goodsList");
        Elements elements = element.getElementsByTag("li");
        ArrayList<Product> products = new ArrayList<>();
        for (Element element1 : elements) {
            String id = element1.attr("data-sku");
            Elements img1 = element1.getElementsByTag("img").eq(0);
            String img = img1.attr("data-lazy-img");
            String price = element1.getElementsByClass("p-price").eq(0).text();
            String title = element1.getElementsByClass("p-name").eq(0).text();
            String shop = element1.getElementsByClass("p-shop").eq(0).text();
            products.add(new Product(id, img, title, shop, price));
        }
        return products;
    }

}

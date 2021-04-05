package com.yandong025.es.controller;

import com.yandong025.es.pojo.Product;
import com.yandong025.es.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
public class ProductController {

    @Autowired
    private ProductService productService;

    @GetMapping("/parse/{keyword}")
    public boolean parseContent(@PathVariable String keyword) throws IOException {
        return productService.parseContent(keyword);
    }


    @GetMapping("/search/{keyword}/{pageNo}/{size}")
    public List<Product> getProduct(@PathVariable String keyword,
                                    @PathVariable int pageNo,
                                    @PathVariable int size) throws IOException {




        return productService.getProduct(keyword,pageNo,size);
    }

}

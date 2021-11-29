package com.ollie.unifieddriverupgrade;

import com.ollie.unifieddriverupgrade.mapper.Product;
import com.ollie.unifieddriverupgrade.service.ProductService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProductController {

    private ProductService productService;

    @Autowired
    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @RequestMapping("/product")
    public Product product(@RequestParam(value = "id") String id) {
        return productService.getProduct(id);
    }
}

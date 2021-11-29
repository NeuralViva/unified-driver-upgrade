package com.ollie.unifieddriverupgrade.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

import com.ollie.unifieddriverupgrade.mapper.Product;
import com.ollie.unifieddriverupgrade.mapper.ProductDao;

@Component
public class ProductService {
	private ProductDao productDao;

	@Autowired
	public ProductService(ProductDao productDao) {
		this.productDao = productDao;
	}

	public Product getProduct(String id) {
		return productDao.findById(UUID.fromString(id));
	}

	public void saveProduct(Product product) {
		productDao.save(product);
	}
}

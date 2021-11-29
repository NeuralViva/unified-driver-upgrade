package com.ollie.unifieddriverupgrade.mapper;

import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Select;

@Dao
public interface ProductDao {
	@Select
	Product findById(UUID id);

	@Insert
	void save(Product product);

	@Delete(entityClass = Product.class)
	void delete(Product product);
}

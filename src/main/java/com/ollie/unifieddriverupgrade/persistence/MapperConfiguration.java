package com.ollie.unifieddriverupgrade.persistence;

import com.datastax.oss.driver.api.core.CqlSession;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapper;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapperBuilder;
import com.ollie.unifieddriverupgrade.mapper.ProductDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MapperConfiguration {

	@Bean
	public InventoryMapper inventoryMapper(@Autowired CqlSession cqlSession) {
		return new InventoryMapperBuilder(cqlSession).build();
	}

	@Bean
	public ProductDao productDao(@Autowired CqlSession cqlSession, @Autowired InventoryMapper inventoryMapper) {
		return inventoryMapper.productDao(cqlSession.getKeyspace().get());
	}
}

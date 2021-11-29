package com.ollie.unifieddriverupgrade.mapper;

import java.util.UUID;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

@Entity
public class Product {

	@PartitionKey
	private UUID id;
	private String description;

	public Product() {
	}

	public Product(UUID id, String description) {
		this.id = id;
		this.description = description;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}

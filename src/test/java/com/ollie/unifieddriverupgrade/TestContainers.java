package com.ollie.unifieddriverupgrade;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapper;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapperBuilder;
import com.ollie.unifieddriverupgrade.mapper.Product;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(SpringJUnit4ClassRunner.class)
public class TestContainers {

	private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:4.0.1");
	private static String address;
	private static Integer port;

	@ClassRule
	public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(CASSANDRA_IMAGE)
			.withInitScript("BDScripts/simple.cql");

	@BeforeClass
	public static void setUp() {
		cassandraContainer.start();
		address = cassandraContainer.getHost();
		port = cassandraContainer.getFirstMappedPort();
	}

	@Test
	public void canRetrieveARecordViaASimpleStatement() throws Exception {

		try (CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.addContactPoint(new InetSocketAddress(address, port))
				.withKeyspace("inventory")
				.build()) {

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
			SimpleStatement statement = SimpleStatement
					.newInstance("select id,description from inventory.product");

			ResultSet result = session.execute(statement);
			Row r = result.one();
			UUID res = r.getUuid("id");
			String resPayload = r.getString("description");

			assertEquals(UUID.fromString(productId), res);
			assertEquals("Laptop", resPayload);
		}
	}

	@Test
	public void canRetrieveARecordViaABoundStatement() throws Exception {

		try (CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.addContactPoint(new InetSocketAddress(address, port))
				.withKeyspace("inventory")
				.build()) {

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
			PreparedStatement prepared = session.prepare("select * from inventory.product where id = ?");
			BoundStatement bound = prepared.bind(UUID.fromString(productId));

			ResultSet result = session.execute(bound);
			Row r = result.one();
			UUID res = r.getUuid("id");
			String resPayload = r.getString("description");

			assertEquals(UUID.fromString(productId), res);
			assertEquals("Laptop", resPayload);
		}
	}

	@Test
	public void canRetrieveRecordViaTheDAO() {
		try (CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.addContactPoint(new InetSocketAddress(address, port))
				.withKeyspace("inventory")
				.build()) {
			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";

			InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
			Product product = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
					.findById(UUID.fromString(productId));

			assertEquals("Laptop", product.getDescription());
		}
	}

	@Test
	public void canCreateAndRetrieveANewRecordViaTheDAO() {
		try (CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.addContactPoint(new InetSocketAddress(address, port))
				.withKeyspace("inventory")
				.build()) {

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3b";

			Product product = new Product();
			product.setId(UUID.fromString(productId));
			product.setDescription("Keyboard");

			InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
			inventoryMapper.productDao(CqlIdentifier.fromCql("inventory")).save(product);
			Product newProduct = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
					.findById(UUID.fromString(productId));

			assertEquals("Keyboard", newProduct.getDescription());
		}
	}

	@Test
	public void canRetrieveRecordViaTheDAOUsingACQLSession() {

		try (CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.addContactPoint(new InetSocketAddress(address, port))
				.withKeyspace("inventory")
				.build()) {

			InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
			Product product = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
					.findById(UUID.fromString(productId));

			assertEquals("Laptop", product.getDescription());
		}
	}
}

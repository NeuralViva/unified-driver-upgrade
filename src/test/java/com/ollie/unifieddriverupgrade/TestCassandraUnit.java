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

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@EmbeddedCassandra
@TestExecutionListeners(listeners = CassandraUnitTestExecutionListener.class)
@CassandraDataSet(value = "BDScripts/simple.cql", keyspace = "inventory")
@Ignore
public class TestCassandraUnit {

	private CqlSession session;
	private InventoryMapper inventoryMapper;

	@Before
	public void setUp() {
		session = EmbeddedCassandraServerHelper.getSession();
		inventoryMapper = new InventoryMapperBuilder(session).build();
	}

	@Test
	public void canRetrieveARecordViaASimpleStatement() throws Exception {

		final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
		SimpleStatement statement = SimpleStatement.newInstance("select id,description from inventory.product");

		ResultSet result = session.execute(statement);
		Row r = result.one();
		UUID res = r.getUuid("id");
		String resPayload = r.getString("description");

		assertEquals(UUID.fromString(productId), res);
		assertEquals("Laptop", resPayload);
	}

	@Test
	public void canRetrieveARecordViaABoundStatement() throws Exception {

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

	@Test
	public void canRetrieveRecordViaTheDAO() {

		final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
		Product product = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
				.findById(UUID.fromString(productId));

		assertEquals("Laptop", product.getDescription());
	}

	@Test
	public void canCreateAndRetrieveANewRecordViaTheDAO() {

		final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3b";

		Product product = new Product();
		product.setId(UUID.fromString(productId));
		product.setDescription("Keyboard");

		inventoryMapper.productDao(CqlIdentifier.fromCql("inventory")).save(product);
		Product newProduct = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
				.findById(UUID.fromString(productId));

		assertEquals("Keyboard", newProduct.getDescription());
	}

	@Test
	public void canRetrieveRecordViaTheDAOUsingACQLSession() {

		try (CqlSession localSession = CqlSession.builder()
				.addContactPoint(new InetSocketAddress("127.0.0.1", 9142))
				.withLocalDatacenter("datacenter1").build()) {

			InventoryMapper localInventoryMapper = new InventoryMapperBuilder(localSession).build();

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
			Product product = localInventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
					.findById(UUID.fromString(productId));

			assertEquals("Laptop", product.getDescription());
		}
	}
}

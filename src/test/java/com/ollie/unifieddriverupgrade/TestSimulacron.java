package com.ollie.unifieddriverupgrade;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapper;
import com.ollie.unifieddriverupgrade.mapper.InventoryMapperBuilder;
import com.ollie.unifieddriverupgrade.mapper.Product;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@Ignore
public class TestSimulacron {

	static Server server = Server.builder().build();
	TypeCodec<UUID> defaultCodec = new CqlTextToUUIDCodec();

	@Test
	public void canRetrieveARecordViaASimpleStatement() throws Exception {

		try (BoundNode node = server.register(NodeSpec.builder())) {

			InetSocketAddress address = (InetSocketAddress) node.getAddress();
			CqlSession session = CqlSession.builder()
					.addContactPoint(
							new InetSocketAddress(address.getHostName(), address.getPort()))
					.withLocalDatacenter("dummy")
					.build();

			final String productId = "c37d661d-7e61-49ea-96a5-68c34e83db3a";
			SimpleStatement statement = SimpleStatement.newInstance(
					"select * from inventory.product where id IN " + productId);
			node.prime(when("select * from inventory.product where id IN " + productId)
					.then(rows().row("id", UUID.fromString("c37d661d-7e61-49ea-96a5-68c34e83db3a"),
							"description", "Laptop")
							.columnTypes("id", "uuid", "description", "varchar")));

			ResultSet result = session.execute(statement);
			Row r = result.one();
			UUID res = r.getUuid("id");
			String resPayload = r.getString("description");

			assertEquals(UUID.fromString(productId), res);
			assertEquals("Laptop", resPayload);
		}
	}

	@Test
	@Ignore("Test fails as Prepared statements with params fail on Simulacron, test retained for reference")
	public void canRetrieveARecordViaABoundStatement() throws Exception {

		try (BoundNode node = server.register(NodeSpec.builder())) {

			InetSocketAddress address = (InetSocketAddress) node.getAddress();
			CqlSession session = CqlSession.builder()
					.addContactPoint(
							new InetSocketAddress(address.getHostName(), address.getPort()))
					.withLocalDatacenter("dummy")
					.addTypeCodecs(defaultCodec)
					.withRequestTracker(null)
					.build();

			final UUID productId = UUID.fromString("c37d661d-7e61-49ea-96a5-68c34e83db3a");
			Select select = selectFrom("inventory", "product")
					.all()
					.whereColumn("id").isEqualTo(bindMarker());
			node.prime(when(select.asCql())
					.then(rows().row("id", productId, "description", "Laptop")
							.columnTypes("id", "uuid", "description", "varchar")));

			PreparedStatement prepared = session.prepare("select * from inventory.product where id = ?");
			BoundStatement bound = prepared.bind(productId);

			ResultSet result = session.execute(bound);
			Row r = result.one();
			UUID res = r.getUuid("id");
			String resPayload = r.getString("description");

			assertEquals(productId, res);
			assertEquals("Laptop", resPayload);
		}
	}

	@Test
	@Ignore("Test fails as Prepared statements with params fail on Simulacron, test retained for reference")
	public void canRetrieveRecordViaTheDAO() {

		try (BoundNode node = server.register(NodeSpec.builder())) {

			InetSocketAddress address = (InetSocketAddress) node.getAddress();
			CqlSession session = CqlSession.builder()
					.addContactPoint(
							new InetSocketAddress(address.getHostName(), address.getPort()))
					.withLocalDatacenter("dummy")
					.addTypeCodecs(defaultCodec)
					.withRequestTracker(null)
					.build();

			final UUID productId = UUID.fromString("c37d661d-7e61-49ea-96a5-68c34e83db3a");
			Select select = selectFrom("inventory", "product")
					.column("id")
					.column("description")
					.whereColumn("id").isEqualTo(bindMarker());

			node.prime(when(select.asCql())
					.then(rows().row("id", productId, "description", "Laptop")
							.columnTypes("id", "uuid", "description", "varchar")));

			InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
			Product product = inventoryMapper.productDao(CqlIdentifier.fromCql("inventory"))
					.findById(productId);

			assertEquals("Laptop", product.getDescription());
		}
	}
}

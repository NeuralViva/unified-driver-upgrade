package com.ollie.unifieddriverupgrade;

import java.util.UUID;

import javax.annotation.Nullable;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

public class CqlTextToUUIDCodec extends MappingCodec<String, UUID> {

	public CqlTextToUUIDCodec() {
		super(TypeCodecs.TEXT, GenericType.UUID);
	}

	@Nullable
	@Override
	protected String outerToInner(@Nullable UUID value) {
		return value == null ? null : value.toString();
	}

	@Nullable
	@Override
	protected UUID innerToOuter(@Nullable String value) {
		return value == null ? null : UUID.fromString(value);
	}
}

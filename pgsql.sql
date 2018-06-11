CREATE SEQUENCE IF NOT EXISTS object_field_position_seq START WITH 1;

CREATE TABLE IF NOT EXISTS object (
	id serial primary key,
	class varchar not null,
	key varchar not null,
	source varchar,
	created timestamp without time zone default NOW(),
	last_modified timestamp without time zone default NOW()
);

CREATE INDEX IF NOT EXISTS object_idx_source ON object (lower(source));
CREATE INDEX IF NOT EXISTS object_idx_class_key ON object (class, key);
CREATE UNIQUE INDEX IF NOT EXISTS object_idx_class_key_lower
	ON object (lower(class), lower(key));

CREATE TABLE IF NOT EXISTS object_field (
	id serial primary key,
	key varchar not null,
	value text not null,
	object_id integer not null references object(id) on delete cascade,
	position integer not null default nextval('object_field_position_seq')
);

CREATE INDEX IF NOT EXISTS object_field_idx_object_id
	ON object_field (object_id);

CREATE OR REPLACE VIEW full_object (object_id, object_class, object_key,
	object_source, object_created, object_last_modified, field_key, field_value,
	field_position)
	AS SELECT object.id AS object_id, object.class AS object_class,
			object.key AS object_key, object.source AS object_source,
			object.created AS object_created,
			object.last_modified AS object_last_modified,
			object_field.key AS field_key, object_field.value as field_value,
			object_field.position AS field_position
		FROM object LEFT JOIN object_field ON object.id = object_field.object_id
		ORDER BY (object.id, position);

CREATE TABLE IF NOT EXISTS route (
	object_id integer not null references object(id) on delete cascade,
	address cidr not null,
	asn bigint not null,
	unique (address, asn)
);

CREATE INDEX IF NOT EXISTS route_idx_object_id ON route (object_id);
CREATE INDEX IF NOT EXISTS route_idx_asn ON route (asn);
CREATE INDEX IF NOT EXISTS route_idx_address
	ON route USING GIST (address inet_ops);

CREATE TABLE IF NOT EXISTS inetnum (
	object_id integer not null references object(id) on delete cascade,
	address cidr unique not null
);

CREATE INDEX IF NOT EXISTS inetnum_idx_object_id ON inetnum (object_id);
CREATE INDEX IF NOT EXISTS inetnum_idx_address
	ON inetnum USING GIST (address inet_ops);

CREATE TABLE IF NOT EXISTS inverse_field (
	object_id integer not null references object(id) on delete cascade,
	key varchar not null,
	value varchar not null,
	unique(object_id, key, value)
);

CREATE INDEX IF NOT EXISTS inverse_field_idx_object_id
	ON inverse_field (object_id);
CREATE INDEX IF NOT EXISTS inverse_field_idx_key_value
	ON inverse_field(key, value);

CREATE TABLE IF NOT EXISTS source (
	name varchar primary key,
	serial integer default 0,
	object_id integer not null references object(id) on delete cascade
);

CREATE UNIQUE INDEX IF NOT EXISTS source_idx_name_lower
	ON source (lower(name));
CREATE INDEX IF NOT EXISTS source_idx_name ON source (name);

CREATE TABLE IF NOT EXISTS as_block (
	object_id integer not null references object(id) on delete cascade,
	range int8range unique not null
);

CREATE INDEX IF NOT EXISTS as_block_idx_range ON as_block
	USING GIST (range);

CREATE INDEX IF NOT EXISTS as_block_idx_object_id ON as_block (object_id);

CREATE TABLE IF NOT EXISTS domain (
	object_id integer not null references object(id) on delete cascade,
	name varchar unique not null
);

CREATE INDEX IF NOT EXISTS domain_idx_object_id ON domain (object_id);
CREATE INDEX IF NOT EXISTS domain_idx_name_rev ON domain (reverse(name));


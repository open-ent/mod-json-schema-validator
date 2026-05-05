/*
 * Copyright © WebServices pour l'Éducation, 2018
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.opendigitaleducation.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.vertx.java.busmods.BusModBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static fr.wseduc.webutils.Utils.getOrElse;
import static fr.wseduc.webutils.Utils.isEmpty;


public class JsonSchemaValidator extends BusModBase implements Handler<Message<JsonObject>> {

	private Map<String, JsonSchema> schemas;
	private JsonSchemaFactory schemaFactory;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Override
	public void start() {
		super.start();
		this.schemas = new HashMap<>();
		this.schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
		vertx.eventBus().consumer(config.getString("address", "json.schema.validator"), this);
	}

	@Override
	public void handle(Message<JsonObject> message) {
		String action = message.body().getString("action", "");
		vertx.executeBlocking(() -> {
			switch (action) {
				case "validate" :
					validate(message);
					break;
				case "getSchemaKeys" :
					getSchemaKeys(message);
					break;
				case "addSchema" :
					addSchema(message);
					break;
				default :
					sendError(message, "invalid.action");
			}
			return null;
		});
	}

	private void validate(Message<JsonObject> message) {
		final String key = message.body().getString("key");
		if (isEmpty(key)) {
			sendError(message, "invalid.key");
			return;
		}
		final JsonSchema schema = schemas.get(key);
		if (schema == null) {
      logger.warn("The schema " + key + " does not exist");
			sendError(message, "invalid.schema.key");
			return;
		}
		final Object json = message.body().getValue("json");
		if (json == null) {
			sendError(message, "missing.json");
			return;
		}
		try {
			final Set<ValidationMessage> errors = schema.validate(OBJECT_MAPPER.readTree(Json.encode(json)));
			if (errors.isEmpty()) {
				sendOK(message);
			} else {
				sendError(message, errors.toString());
			}
		} catch (Exception e) {
			sendError(message, "validation.error", e);
		}
	}

	private void getSchemaKeys(Message<JsonObject> message) {
		sendOK(message, new JsonObject().put("schemas", new JsonArray(new ArrayList<>(schemas.keySet()))));
	}

	private void addSchema(Message<JsonObject> message) {
		final String key = message.body().getString("key");
		if (isEmpty(key)) {
			sendError(message, "invalid.key");
			return;
		}
		if (schemas.containsKey(key) && !getOrElse(message.body().getBoolean("overwrite"), false)) {
			sendError(message, "key.already.exists");
			return;
		}
		final JsonObject schema = message.body().getJsonObject("jsonSchema");
		if (schema == null || schema.size() == 0) {
			sendError(message, "invalid.schema");
			return;
		}
		try {
			final JsonNode jsonNode = OBJECT_MAPPER.readTree(schema.encode());
			final JsonSchema jsonSchema = schemaFactory.getSchema(jsonNode);
			schemas.put(key, jsonSchema);
			sendOK(message);
		} catch (Exception e) {
			sendError(message, "schema.error", e);
		}
	}

}
